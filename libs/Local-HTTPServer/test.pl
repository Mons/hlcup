#!/usr/bin/env perl

use 5.016;
use lib qw(blib/lib blib/arch);
use EV;
use Local::HTTPServer;
use DDP;

my $server = Local::HTTPServer->new("0.0.0.0",8880, sub {
	# p @_;
	# my $req = shift;
	# my $res = shift;
	# $reply->(200,"Test\n",1);
	return 200, "XXXX\n";
});

$server->listen();

$server->accept();

EV::loop;
