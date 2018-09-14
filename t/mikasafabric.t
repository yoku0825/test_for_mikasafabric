#!/usr/bin/perl

use strict;
use warnings;
use utf8;

use FindBin qw{$Bin};
use lib "$Bin/../lib";
use Fabric;
use Server;


use Test::More;
use Data::Dumper;
use JSON;
use Carp::Always;

my $group_name= "myfabric";
my $max_router_retry= 20;
my $router_sleep= 3;

### Run 3 times, because facing mysqlrouter's restart problem.
foreach (1..3)
{
  subtest "test round no. $_" => sub
  {
    system("mikasafabric manage stop");
    system("mikasafabric manage teardown");
    system("mikasafabric manage setup");
    system("mikasafabric manage start --daemonize");
    
    
    my $fabric= Fabric->new($group_name);
    ok($fabric, "Startup mikasafabric");
    
    is_deeply($fabric->lookup_groups, [[$group_name, undef, 'INACTIVE', undef]], "Create group");
    
    my @servers;
    my $master;
    
    for (my $n= 1; $n <= 3; $n++)
    {
      my $id= `docker run -d --hostname="mysql-server$n" yoku0825/mysql_fabric_aware`;
      chomp($id);
      my $ipaddr= `docker inspect -f "{{.NetworkSettings.IPAddress}}" $id`;
      chomp($ipaddr);
    
      my $server= Server->new($id, $ipaddr);
      $fabric->add($server);
      push(@servers, $server);
    
      subtest "mysqld$n" => sub
      {
        is($fabric->lookup_servers($server)->[2], "SPARE", "$server->{host_port} has been added into mikasafabric");
        $fabric->set_status($server, "SECONDARY");
    
        is($fabric->lookup_servers($server)->[2], "SECONDARY", "$server->{host_port} get to be SECONDARY");
        $fabric->promote($server);
    
        is($fabric->lookup_master, $server->{uuid}, "$server->{host_port} has been promoted");
        $master->{is_master}= 0 if $master;
        $server->{is_master}= 1;
        $master= $server;
        done_testing;
      }
    }
    
    ok(healthcheck($fabric, @servers), "mikasafabric cluster startup");
    
    subtest "writing master" => sub
    {
      $master->get_conn->do("CREATE DATABASE ap");
      $master->get_conn->do("CREATE TABLE ap.t1 (num serial, val varchar(32))");
      $master->get_conn->do("INSERT INTO ap.t1 VALUES (1, 'one')");
      sleep 1;
    
      ok(healthcheck(@servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
    
      $master->get_conn->do("INSERT INTO ap.t1 VALUES (2, 'two')");
      done_testing;
    };
    
    subtest "promote" => sub
    {
      $master->get_conn->do("INSERT INTO ap.t1 VALUES (3, 'three')");
      $fabric->promote;
    
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
      done_testing;
    };
    
    system("systemctl restart mysqlrouter");
    sleep 5;
    my $router_write= Server->new(undef, "127.0.0.1", 13306);
    my $router_read = Server->new(undef, "127.0.0.1", 23306);
    
    subtest "write master via router" => sub
    {
      is($router_write->get_conn->selectrow_arrayref("SHOW VARIABLES LIKE 'server_uuid'")->[1],
         $fabric->lookup_master, "Router points to master");
      $router_write->get_conn->do("INSERT INTO ap.t1 VALUES (4, 'four')");
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
      done_testing;
    };
    
    subtest "read slave via router" => sub
    {
      my $uuid;
      foreach (1..$max_router_retry)
      {
        $uuid->{$router_read->get_uuid}= 1;
        sleep $router_sleep;
      }
      my $uuid_str= Dumper($uuid);
      is(scalar(keys(%$uuid)), 2, "Round-robbined 2 servers(non-allow_primary_read) $uuid_str");
      $uuid= {};
      $uuid_str= "";
    
      $fabric->set_status($master, "SPARE");
      my $ng= 0;
      foreach (1..$max_router_retry)
      {
        $ng++ if $router_read->get_uuid eq $master->{uuid};
        sleep $router_sleep;
      }
      isnt($ng, $max_router_retry / 2, "Server is not devided $ng times");
     
      $fabric->set_status($master, "SECONDARY");
      foreach (1..$max_router_retry)
      {
        $uuid->{$router_read->get_uuid}= 1;
        sleep $router_sleep;
      }
      $uuid_str= Dumper($uuid);
      is(scalar(keys(%$uuid)), 2, "Round-robbined 2 servers(non-allow_primary_read) $uuid_str");
    
      done_testing;
    };
     
    subtest "server faulty" => sub
    {
      ### old-master, maybe slave.
      $master->use_root->do("SET GLOBAL offline_mode= 1");
      @servers= remove_server($master, @servers);
      $fabric->wait_fd;
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
    
      my @ret= sort(map { $_->[2] } @{$fabric->lookup_servers});
      is_deeply(\@ret, ["FAULTY", "PRIMARY", "SECONDARY"], "Status is correct");
    
      $router_write->get_conn->do("INSERT INTO ap.t1 VALUES (5, 'five')");
    
      my $ng= 0;
      foreach (1..$max_router_retry)
      {
        $ng++ if $router_read->get_uuid eq $master->{uuid};
        sleep $router_sleep;
      }
      isnt($ng, $max_router_retry / 2, "Server is not devided $ng times");
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
    
      done_testing;
    };
    
    subtest "server makes alive" => sub
    {
      $master->use_root->do("SET GLOBAL offline_mode= 0");
      push(@servers, $master);
      $router_write->get_conn->do("INSERT INTO ap.t1 VALUES (6, 'six')");
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
      $fabric->set_status($master, "SPARE");
      is($fabric->lookup_servers($master)->[2], "SPARE", "old-master returns SPARE");
    
      $router_write->get_conn->do("INSERT INTO ap.t1 VALUES (7, 'seven')");
    
      my $ng= 0;
      foreach (1..$max_router_retry)
      {
        $ng++ if $router_read->get_uuid eq $master->{uuid};
        sleep $router_sleep;
      }
      isnt($ng, $max_router_retry / 2, "Server is not devided $ng times");
     
      $fabric->set_status($master, "SECONDARY");
      is($fabric->lookup_servers($master)->[2], "SECONDARY", "old-master returns SECONDARY");
    
      my $ok= 0;
      foreach (1..$max_router_retry)
      {
        $ok= 1 if $router_read->get_uuid eq $master->{uuid};
        sleep $router_sleep;
      }
      ok($ok, "SECONDARY Server is back to round-robin routing");
    
      $fabric->promote($master);
      is($fabric->lookup_servers($master)->[2], "PRIMARY", "old-master returns PRIMARY");
      $router_write->get_conn->do("INSERT INTO ap.t1 VALUES (8, 'eight')");
      is($router_write->get_uuid, $master->{uuid}, "Router back to point to master");
    
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
      done_testing;
    };
    
    subtest "dead migration" => sub
    {
      $master->use_root->do("SHUTDOWN");
      @servers= remove_server($master, @servers);
      $fabric->wait_fd;
      ok(healthcheck($fabric, @servers), "Not broken yet");
      ok(is_synced("SELECT * FROM ap.t1", @servers), "Data is synced");
      my @ret= sort(map { $_->[2] } @{$fabric->lookup_servers});
      is_deeply(\@ret, ["FAULTY", "PRIMARY", "SECONDARY"], "Status is correct");
    };
  };
}

done_testing;






sub healthcheck
{
  my (@servers)= @_;
  my $ng= 0;

  foreach (@servers)
  {
    $ng= 1 if $_->healthcheck ne "OK";
  }

  return !($ng);
}

sub is_synced
{
  my ($sql, @servers)= @_;
  my $ng= 0;

  my @ret= map { Dumper($_->get_conn->selectall_arrayref($sql)) } @servers;

  for (my $n= 0; $n <= $#ret; $n++)
  {
    for (my $m= $n + 1; $m <= $#ret; $m++)
    {
      $ng= 1 if $ret[$n] ne $ret[$m]
    }
  }
  return !($ng);
}

sub remove_server
{
  my ($server, @servers)= @_;

  for (my $n= 0; $n <= $#servers; $n++)
  {
    delete($servers[$n]) if $server->{uuid} eq $servers[$n]->{uuid};
  }
  return @servers;
}


