#!/usr/bin/perl

use strict;
use warnings;
use utf8;

use Ytkit::HealthCheck;
use Test::More;
use Data::Dumper;
use JSON;

system("mikasafabric manage stop");
system("mikasafabric manage teardown");
system("mikasafabric manage setup");
system("mikasafabric manage start --daemonize");

my $group_name= "myfabric";
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
  $master->{conn}->do("CREATE DATABASE d1");
  $master->{conn}->do("CREATE TABLE d1.t1 (num serial, val varchar(32))");
  $master->{conn}->do("INSERT INTO d1.t1 VALUES (1, 'one')");
  sleep 1;

  ok(healthcheck(@servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");

  $master->{conn}->do("INSERT INTO d1.t1 VALUES (2, 'two')");
  done_testing;
};

subtest "promote" => sub
{
  $master->{conn}->do("INSERT INTO d1.t1 VALUES (3, 'three')");
  $fabric->promote;

  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");
  done_testing;
};

system("systemctl restart mysqlrouter");
my $router_write= Server->new(undef, "127.0.0.1", 13306);
my $router_read = Server->new(undef, "127.0.0.1", 23306);

subtest "write master via router" => sub
{
  is($router_write->{conn}->selectrow_arrayref("SHOW VARIABLES LIKE 'server_uuid'")->[1],
     $fabric->lookup_master, "Router points to master");
  $router_write->{conn}->do("INSERT INTO d1.t1 VALUES (4, 'four')");
  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");
  done_testing;
};

subtest "server faulty" => sub
{
  ### old-master, maybe slave.
  $master->{conn}->do("SET GLOBAL offline_mode= 1");
  @servers= remove_server($master, @servers);
  sleep 5;
  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");

  my @ret= sort(map { $_->[2] } @{$fabric->lookup_servers});
  is_deeply(\@ret, ["FAULTY", "PRIMARY", "SECONDARY"], "Status is correct");

  $router_write->{conn}->do("INSERT INTO d1.t1 VALUES (5, 'five')");

  foreach (1..10)
  {
    ok(0, "Server is not devided") 
      if $router_read->get_uuid eq $master->{uuid};
  }
  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");

  done_testing;
};

subtest "server makes alive" => sub
{
  $master->{conn}->do("SET GLOBAL offline_mode= 0");
  push(@servers, $master);
  $router_write->{conn}->do("INSERT INTO d1.t1 VALUES (6, 'six')");
  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");
  $fabric->set_status($master, "SPARE");
  is($fabric->lookup_servers($master)->[2], "SPARE", "old-master returns SPARE");

  $router_write->{conn}->do("INSERT INTO d1.t1 VALUES (7, 'seven')");

  foreach (1..10)
  {
    ok(0, "Server is not devided") 
      if $router_read->get_uuid eq $master->{uuid};
  }
 
  $fabric->set_status($master, "SECONDARY");
  is($fabric->lookup_servers($master)->[2], "SECONDARY", "old-master returns SECONDARY");

  my $ok= 0;
  foreach (1..10)
  {
    $ok= 1 if $router_read->get_uuid eq $master->{uuid};
  }
  ok($ok, "SECONDARY Server is back to round-robin routing");

  $fabric->promote($master);
  is($fabric->lookup_servers($master)->[2], "PRIMARY", "old-master returns PRIMARY");
  $router_write->{conn}->do("INSERT INTO d1.t1 VALUES (8, 'eight')");
  is($router_write->get_uuid, $master->{uuid}, "Router back to point to master");

  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");
  done_testing;
};

subtest "dead migration" => sub
{
  $master->{conn}->do("SHUTDOWN");
  @servers= remove_server($master, @servers);
  sleep 5;
  ok(healthcheck($fabric, @servers), "Not broken yet");
  ok(is_synced("SELECT * FROM d1.t1", @servers), "Data is synced");
  my @ret= sort(map { $_->[2] } @{$fabric->lookup_servers});
  is_deeply(\@ret, ["FAULTY", "PRIMARY", "SECONDARY"], "Status is correct");
};

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

  my @ret= map { Dumper($_->{conn}->selectall_arrayref($sql)) } @servers;

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

package Fabric;

use strict;
use warnings;
use utf8;
use DBI;
use Carp;
use Ytkit::HealthCheck;

sub new
{
  my ($class, $group)= @_;
  my $dsn= "dbi:mysql:;host=127.0.0.1;port=32275";

  my $conn;
  for (my $n= 1; $n <= 30; $n++)
  {
    eval
    {
      $conn= DBI->connect($dsn, "admin", "", { RaiseError => 1, PrintError => 0, mysql_auto_reconnect => 1 });
    };
    last if !($@);
    sleep 1;
  }

  my $self=
  {
    group => $group,
    conn => $conn,
    _servers => [],
  };
  bless $self => $class;

  $self->_query("CALL group.create('$group')");

  return $self;
}

sub healthcheck
{
  my ($self)= @_;
  return Ytkit::HealthCheck->new("--host=127.0.0.1", "--port=32275", "--role=fabric")->{status}->{str};
}

sub lookup_groups
{
  my ($self)= @_;
  return $self->_query("CALL group.lookup_groups()");
}

sub lookup_servers
{
  my ($self, $server)= @_;
  my $sql= sprintf("CALL group.lookup_servers('%s')", $self->{group});
  my $rs= $self->_query($sql);

  if ($server)
  {
    foreach (@$rs)
    {
      return $_ if $_->[0] eq $server->{uuid};
    }
    return undef;
  }
  else
  {
    return $rs;
  }
}

sub create_group
{
  my ($self, $group)= @_;
  return $self->_query("CALL group.create('$group')");
}

sub add
{
  my ($self, $server)= @_;
  my $sql= sprintf("CALL group.add('%s', '%s')", $self->{group}, $server->{host_port});
  push(@{$self->{_servers}}, $server);

  return $self->_query($sql);
}

sub set_status
{
  my ($self, $server, $status)= @_;
  my $sql= sprintf("CALL server.set_status('%s', '%s')", $server->{host_port}, $status);
  return $self->_query($sql);
}

sub promote
{
  my ($self, $server)= @_;
  my $arg= $server ? sprintf("'%s', '%s'", $self->{group}, $server->{uuid}) : $self->{group};
  $self->_query("CALL group.promote($arg)");
  return $self->lookup_master;
}

sub lookup_master
{
  my ($self)= @_;
  foreach (@{$self->lookup_servers})
  {
    return $_->[0] if $_->[2] eq "PRIMARY";
  }
  return undef;
}

sub _query
{
  my ($self, $sql)= @_;
  my $rs;

  eval
  {
    my $stmt= $self->{conn}->prepare($sql);
    $stmt->execute;

    $rs= $stmt->fetchall_arrayref;
    $stmt->more_results;
    $rs= $stmt->fetchall_arrayref;
  };
  carp($@) if $@;
  return $rs;
}

package Server;

use strict;
use warnings;
use utf8;
use DBI;
use Ytkit::HealthCheck;

sub new
{
  my ($class, $id, $host, $port)= @_;
  $port //= 3306;
  my $dsn= "dbi:mysql:;host=$host;port=$port";

  my $conn;
  for (my $n= 1; $n <= 30; $n++)
  {
    eval
    {
      $conn= DBI->connect($dsn, "root", "", { RaiseError => 1, PrintError => 0, mysql_auto_reconnect => 1 });
    };
    last if !($@);
    sleep 1;
  }

  my $self=
  {
    host => $host,
    port => $port,
    conn => $conn,
    docker_id => $id,
    host_port => "$host:$port",
    is_master => 0,
  };
  bless $self => $class;
  $self->{uuid}= $self->get_uuid;

  return $self;
}

sub get_uuid
{
  my ($self)= @_;
  my $dsn= sprintf("dbi:mysql:;host=%s;port=%d", $self->{host}, $self->{port});
  my $conn= DBI->connect($dsn, "root", "", { RaiseError => 1, PrintError => 0, mysql_auto_reconnect => 1 });
  return $conn->selectrow_arrayref("SHOW VARIABLES LIKE 'server_uuid'")->[1];
}

sub healthcheck
{
  my ($self)= @_;
  return Ytkit::HealthCheck->new("--host", $self->{host},
                                 "--port", $self->{port},
                                 "--role=auto")->{status}->{str};
}

sub DESTROY
{
  my ($self)= @_;
  return if !($self->{docker_id});
  my $id= $self->{docker_id};
  system("docker stop $id");
  system("docker rm $id");
}
