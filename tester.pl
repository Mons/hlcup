#!/urs/bin/env perl

use 5.016;
use AE;
use EV;
use AnyEvent::HTTP;
use DDP;
use URI;
use Test::More;
use JSON::XS;
use lib glob("libs/*/lib"),glob("libs/*/blib/lib"),glob("libs/*/blib/arch");
use HTTP::Easy::Headers;


our $JSON = JSON::XS->new->utf8;

my $phase = 'phase_1_get';
# my $phase = 'phase_2_post';

open my $f, '<:raw', "hlcupdocs/answers/${phase}.answ" or die "$phase.answ: $!";
open my $ammo, '<:raw', "hlcupdocs/ammo/${phase}.ammo" or die "$phase.ammo: $!";

my $next_bullet = sub {
	my $am = <$ammo>
		or return;
	chomp $am;
	my ($bytes, $title) = split ' ',$am, 2;
	# say $am, $bytes, $title;;
	read($ammo, my $buf, $bytes) == $bytes or die "$!";
	
	# p $buf;
	my ($hdrs,$body) = split /\015?\012\015?\012/, $buf, 2;
	$hdrs =~ y/\015//;
	my ($line,$hdr) = split /\012/, $hdrs, 2;
	my ($met,$pq) = split /\s+/,$line;
	my $h = HTTP::Easy::Headers->decode($hdr);
	my $rv = {
		method => $met,
		path_query => $pq,
		headers => {%$h},
		body => $body,
	};
	# p $rv;
	return $rv;
	# p $met;
	# p $pq;
	# p $line;
	# p $hdr;
	# p $body;
};

my $cv = AE::cv;$cv->begin;
my $test;$test = sub {
	my $rec = <$f>
		or return;
	chomp $rec;
	my $am = $next_bullet->()
		or return;


	# p $rec;
	my ($met,$path,$st,$body) = split /\t/,$rec;
	# p $am;
	$am->{path_query} eq $path or die "Mismatch";
	# $test->();


	# exit;
	# say $path;
	my $u = URI->new("http://127.0.0.1:8880$path");
	# $u->path_query($path);
	say "$met $u";
	$cv->begin;
	http_request
		$met => $u,
		headers => { %{ $am->{headers} }, connection => 'close'},
		body => $am->{body},
		sub {
			if ($st == 200) {
				my $ok = 0;
				$ok += is $_[1]{Status},$st, "$met $path $st";
				if ($body) {
					$ok += is_deeply
						$JSON->decode($_[0]),
						$JSON->decode($body),
						"$met $path body";
				}
				else {
					$ok += like $body, qr{^(|\{\})$},"$met $path body";
				}

				unless ($ok == 2) {
					diag $_[1]{Status};
					diag $_[0];
					return $cv->end;
				}
			}
			else {
				unless (is $_[1]{Status},$st, "$met $path $st" ) {
					diag $_[1]{Status};
					diag $_[0];
					return $cv->end;
				}
			}
			$test->();
			$cv->end;
		}
	;

		

};$test->();
$cv->cb(sub { EV::unloop; });
$cv->end;

EV::loop;

# while (<$f>) {
# 	my @x = split /\t/,$_;
# 	p @x;
# 	# say $_;
# 	last;
# }