#!/usr/bin/perl

use strict;
use warnings;
use utf8;

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
    $master->{is_master}= false if $master;
    $server->{is_master}= true;
    $master= $server;
    done_testing;
  }
}






done_testing;



package Fabric;

use strict;
use warnings;
use utf8;
use DBI;
use Carp;

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
    is_master => false,
  };
  bless $self => $class;

  return $self;
}

