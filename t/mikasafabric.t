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

    is($fabric->lookup_servers($server)->[2], "PRIMARY", "$server->{host_port} has been promoted");
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
      $conn= DBI->connect($dsn, "admin", "", { RaiseError => 1, PrintError => 0 });
    };
    last if !($@);
    sleep 1;
  }

  my $self=
  {
    group => $group,
    conn => $conn,
  };
  bless $self => $class;

  $self->_query("CALL group.create('$group')");

  return $self;
}

sub healthcheck
{
  my ($self)= @_;
  $self->{_health} //= Ytkit::HealthCheck->new("--host=127.0.0.1", "--port=32275", "--role=fabric");
  return $self->{_health}->{status}->{str};
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
  return $self->_query("CALL group.promote($arg)");
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
      $conn= DBI->connect($dsn, "root", "", { RaiseError => 1, PrintError => 0 });
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
    uuid => $conn->selectall_arrayref("SHOW VARIABLES LIKE 'server_uuid'")->[0]->[1],
    is_master => 0,
  };
  bless $self => $class;

  return $self;
}

sub healthcheck
{
  my ($self)= @_;
  $self->{_health} //= Ytkit::HealthCheck->new("--host", $self->{host},
                                               "--port", $self->{port},
                                               "--role=auto");
  return $self->{_health}->{status}->{str};
}

sub DESTROY
{
  my ($self)= @_;
  my $id= $self->{docker_id};
  system("docker stop $id");
  system("docker rm $id");
}
