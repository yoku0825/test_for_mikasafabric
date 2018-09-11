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
      $conn= DBI->connect($dsn, "ap", "", { RaiseError => 1, PrintError => 0, mysql_auto_reconnect => 1 });
    };
    last if !($@);
    sleep 1;
  }

  my $self=
  {
    host => $host,
    port => $port,
    docker_id => $id,
    host_port => "$host:$port",
    is_master => 0,
  };
  bless $self => $class;
  $self->{uuid}= $self->get_uuid;

  return $self;
}

sub get_conn
{
  my ($self)= @_;
  my $dsn= sprintf("dbi:mysql:;host=%s;port=%d", $self->{host}, $self->{port});
  return DBI->connect($dsn, "ap", "", { RaiseError => 1, PrintError => 0, mysql_auto_reconnect => 1 });
}

sub use_root
{
  my ($self)= @_;
  my $dsn= sprintf("dbi:mysql:;host=%s;port=%d", $self->{host}, $self->{port});
  return DBI->connect($dsn, "root", "", { RaiseError => 1, PrintError => 0, mysql_auto_reconnect => 1 });
}



sub get_uuid
{
  my ($self)= @_;
  return $self->get_conn->selectrow_arrayref("SHOW VARIABLES LIKE 'server_uuid'")->[1];
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

return 1;
