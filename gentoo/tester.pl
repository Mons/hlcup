#!/urs/bin/env perl

use 5.016;
use open qw (:utf8 :std);
use AE;
use EV;
use AnyEvent::HTTP;
use DDP;
use URI;
use Test::More;
use JSON::XS;
use lib glob("../libs/*/lib"),glob("../libs/*/blib/lib"),glob("../libs/*/blib/arch");
use HTTP::Easy::Headers;
use Time::Moment;
use Encode qw(decode);
use Test::Deep;
use Time::HiRes 'time';

our $JSON = JSON::XS->new->utf8;

my $source = $ARGV[0] or 'TRAIN';
my $ph = $ARGV[1] or 2;
my $phase = [
	qw(phase_1_get phase_2_post phase_3_get phase_4_bads)
]->[$ph-1] or die;
# my $phase = 'phase_1_get';
# my $phase = 'phase_2_post';
# my $phase = 'phase_3_get';


open my $f, '<:raw', "hlcupdocs/data/$source/answers/${phase}.answ" or die "$phase.answ: $!";
open my $ammo, '<:raw', "hlcupdocs/data/$source/ammo/${phase}.ammo" or die "$phase.ammo: $!";
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

sub dump_user_visits {
	my $name = shift;
	my $av = shift;
	say "--- $name ".(0+@$av);
	for (@$av) {
		printf "%s\t\t@%s\t%20s\t%s\n",
			$_->{id},
			Time::Moment->from_epoch($_->{visited_at}),
			$_->{place},
			$_->{mark},
	}
}

my $start = time;
my $count = 0;
my $cv = AE::cv;$cv->begin;
my $test;$test = sub {
	my $rec = <$f>
		or return;
	chomp $rec;
	my $am = $next_bullet->()
		or return;
	++$count;


	# p $rec;
	my ($met,$path,$st,$body) = split /\t/,$rec;
	# p $am;
	$am->{path_query} eq $path or die "Mismatch $am->{path_query} vs $path";
	# $test->();

	# return $test->() unless $path =~ m{^/location};
	# if ($ph == 2) {
	# 	return $test->()
	# 		if $path !~ m{^/visit} or $JSON->decode($am->{body})->{user} != 166;
	# }

	# exit;
	# say $path;
	my $u = URI->new("http://127.0.0.1:8880$path");
	# $u->path_query($path);
	say "$met $u";
	$cv->begin;
	http_request
		$met => $u,
		headers => { %{ $am->{headers} }, connection => 'close', 'content-length' => length $am->{body}},
		body => $am->{body} && $am->{body}."\015\012",
		sub {
			if ($st == 200) {
				my $ok = 0;
				$ok += is $_[1]{Status},$st, "$met $path $st";
				if ($body) {
					# p $_[0];
					# p $JSON->decode($_[0]);
					# p $JSON->decode($body);

					# my ($got) = eval {$JSON->decode_prefix($_[0])};
					# eval {
					# 	if (ref $got eq 'HASH' and ref $got->{visits} eq 'ARRAY') {
					# 		for (@{ $got->{visits} }) { delete $_->{extra}};
					# 	}
					# };

					my $got = eval {$JSON->decode($_[0])};
					my $exp = $JSON->decode($body);
					$ok += cmp_deeply
						$got,
						$exp,
						"$met $path body"
						or diag $body;
				}
				else {
					$ok += like $_[0], qr{^(|\{\})$},"$met $path body";
				}

				unless ($ok == 2) {
					diag $_[1]{Status};
					diag decode utf8 => $_[0];
					if ($met eq 'POST') {
						diag decode utf8 => $am->{body};
					}
					my $jd = $JSON->decode($body);
					if ($jd->{visits}) {
						dump_user_visits "expected", $jd->{visits};
						dump_user_visits "received", $JSON->decode($_[0])->{visits};
					}
					

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
done_testing;
warn sprintf "Processed %d requests in %0.2fs: %0.4fRPS\n", $count, time- $start, $count/(time-$start);

# while (<$f>) {
# 	my @x = split /\t/,$_;
# 	p @x;
# 	# say $_;
# 	last;
# }