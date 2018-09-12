package Fabric;

use strict;
use warnings;
use utf8;
use DBI;
use Carp;
use Data::Dumper;
use Ytkit::HealthCheck;

my $fabric_ttl= 1;
my $ttl_sleep = $fabric_ttl * 3 + 1;
my $fd_time   = 4;
my $fd_sleep  = $fd_time * 2 + $ttl_sleep;

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
  sleep $ttl_sleep;
  return $self->_query("CALL group.create('$group')");
}

sub add
{
  my ($self, $server)= @_;
  my $sql= sprintf("CALL group.add('%s', '%s')", $self->{group}, $server->{host_port});
  push(@{$self->{_servers}}, $server);
  sleep $ttl_sleep;

  return $self->_query($sql);
}

sub set_status
{
  my ($self, $server, $status)= @_;
  my $sql= sprintf("CALL server.set_status('%s', '%s')", $server->{host_port}, $status);
  sleep $ttl_sleep;
  return $self->_query($sql);
}

sub promote
{
  my ($self, $server)= @_;
  my $arg= $server ? sprintf("'%s', '%s'", $self->{group}, $server->{uuid}) : $self->{group};
  $self->_query("CALL group.promote($arg)");
  sleep $ttl_sleep;
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

sub wait_fd
{
  my ($self)= @_;
  sleep $fd_sleep;
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
  carp(Dumper($rs)) if $ENV{VERBOSE};
  return $rs;
}

return 1;
