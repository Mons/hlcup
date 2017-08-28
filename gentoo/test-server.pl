#!/usr/bin/perl

use utf8;
# use open qw(:utf8 :std);
use 5.016;
use JSON::XS;
use DDP;
use Time::HiRes qw(time sleep);
use POSIX qw(WNOHANG);
use List::Util qw(min max);
use Getopt::Long qw(:config bundling gnu_compat);
use AE;
use AnyEvent::Util;
use lib glob("../libs/*/lib"),glob("../libs/*/blib/lib"),glob("../libs/*/blib/arch");
use Time::Moment;
use EV;
use Errno;
use Socket qw(SOL_SOCKET SO_LINGER IPPROTO_TCP TCP_NODELAY TCP_DEFER_ACCEPT TCP_CORK);
use Router::R3;
use URI::XSEscape 'uri_unescape';
use AnyEvent::Socket;
# use Local::HLCup;

my $port;
my $debug;
my $src;
my $options = '/tmp/data/options.txt';
BEGIN {
	$port = 80;
	$src = 'TRAIN';
	GetOptions(
		'p|port=n' => \$port,
		'd|debug+' => \$debug,
		's|source=s' => \$src,
		'o|options=s' => \$options,
	) or die;
}
use constant DEBUG => $debug;
our $JSON = JSON::XS->new->utf8;
our $JSONN = JSON::XS->new->utf8->allow_nonref;

my $min = 1e10;
my $sum = 0;
my $max = 0;
my $cnt = 0;
my $prev = $cnt;
my $last_ac = 'same';
my $CONNS = my $REQS = 0;

my $GET = Router::R3->new(
	'/users/{id:\d+}'         => \&get_user,
	'/visits/{id:\d+}'        => \&get_visit,
	'/locations/{id:\d+}'     => \&get_location,
	'/users/{id:\d+}/visits'  => \&get_user_visits,
	'/locations/{id:\d+}/avg' => \&get_location_avg,
	'/locations/{id:\d+}/visits' => \&get_location_visits,
);
my $POST = Router::R3->new(
	'/users/new'              => \&create_user,
	'/locations/new'          => \&create_location,
	'/visits/new'             => \&create_visit,
	'/users/{id:\d+}'         => \&update_user,
	'/locations/{id:\d+}'     => \&update_location,
	'/visits/{id:\d+}'        => \&update_visit,
);

our %NAMES = (
	0+\&get_user         => "GU",
	0+\&get_visit        => "GV",
	0+\&get_location     => "GL",
	0+\&get_user_visits  => "GUV",
	0+\&get_location_avg => "GLA",
	0+\&create_user      => "CU",
	0+\&create_location  => "CL",
	0+\&create_visit     => "CV",
	0+\&update_user      => "UU",
	0+\&update_location  => "UL",
	0+\&update_visit     => "UV",
);
our %NAMES = (
	0+\&get_user         => "GU",
	0+\&get_visit        => "GV",
	0+\&get_location     => "GL",
	0+\&get_user_visits  => "GUV",
	0+\&get_location_avg => "GLA",
	0+\&create_user      => "CU",
	0+\&create_location  => "CL",
	0+\&create_visit     => "CV",
	0+\&update_user      => "UU",
	0+\&update_location  => "UL",
	0+\&update_visit     => "UV",
);
our %STAT;

use HTTP::Parser::XS qw(parse_http_request);
sub decode_query {
	my %rv;
	return \%rv unless length $_[0];
	for (split '&', $_[0]) {
		my ($k,$v) = split '=', $_, 2;
		$k = uri_unescape($k =~ s/\+/ /sr);
		$v = uri_unescape($v =~ s/\+/ /sr);
		utf8::decode $k;
		utf8::decode $v;
		$rv{ $k } = $v;
	}
	return \%rv;
}

tcp_server 0, $port, sub {
	my $fh = shift;
	# AnyEvent::Util::fh_nonblocking($fh,1);
	++$CONNS;
	setsockopt($fh, SOL_SOCKET, SO_LINGER, 0);
	my $rbuf;
	my $read_time;
	my $read_count = 0;

	my $rwcb;
	my $rw;
	my ($m, $cap, $met,$path_query,%env);

	my $fin = sub {
		close $fh;
		undef $rwcb;
		undef $rw;
		--$CONNS;
	};
	my $start;
	my $reply = sub {
		my ($st,$b, $close) = @_;
		# warn "$met $path $st: $b\n" if DEBUG > 1;
		my $con = $close ? 'close':'keep-alive';
		my $wbuf = "HTTP/1.1 $st X\015\012Server: Perl/5\015\012Connection: $con\015\012Content-Length: ".
			length($b)."\015\012\015\012".$b;
		my $wr = syswrite($fh,$wbuf);

		my $run = time - $start;
		if ( $run > 0.5 ) {
			warn sprintf "[%s:%s] %s %s?%s:%s %0.4fs (%0.4fs,%s) [%s]\n",
				$CONNS, $REQS, $met,$env{PATH_INFO}, $env{QUERY_STRING},
					$st,$run,$read_time,$read_count, $JSONN->encode(\%env);
		}
		$min = min($min,$run);
		$max = max($max,$run);
		$sum += $run;
		$cnt++;
		--$REQS;
		my $N = $NAMES{ 0+$m } // 'OTH';
		$STAT{'C'.$N}++;
		$STAT{'R'.$N}+= $run;

		if ($wr == length $wbuf) {

		}
		else {
			warn "Write failed: $!\n";
			$fin->();
			return;
		}
		if ($close) {
			$fin->();
		} else {
			%env = ();
			# undef $start;
			$start = time;
			$rw = AE::io $fh, 0, $rwcb;
		}
	};

	$rwcb = sub {
		$start //= time;
		my $r = sysread($fh, $rbuf, 256*1024, length $rbuf);
		$read_count++;
		if ($r) {
			$read_time = time - $start;
			my $ret = parse_http_request($rbuf,\%env);
			# p %env;
			if ($ret > 0) {
				$met = $env{REQUEST_METHOD};
				if ($met eq 'GET') {
					$rbuf = substr($rbuf, $ret);
					($m,$cap) = $GET->match($env{PATH_INFO});
					++$REQS;
					$m or return $reply->(404, '{"error":"path"}',0);
					my $query = decode_query($env{QUERY_STRING});
					$reply->(400,'{}',0);
				}
				elsif ($met eq 'POST') {
					if ( $env{CONTENT_LENGTH} + $ret > length $rbuf ) {
						warn "Not enough body: $env{CONTENT_LENGTH} + $ret < length $rbuf";
						return $rw = AE::io $fh, 0, $rwcb;
					}
					($m,$cap) = $POST->match($env{PATH_INFO});
					++$REQS;
					$m or return $reply->(404, '{"error":"path"}', 1);
					my $body = \substr($rbuf, $ret, $env{CONTENT_LENGTH});
					$env{body} = $body;

					my $data;
					eval {
						$data = $JSON->decode($$body);
					1} or do {
						return $reply->(400, '{"error":"bad json"}', 1);
					};
					# p $data;
					my $query = decode_query($env{QUERY_STRING});
					$reply->(400,'{}',1);
				}
				else {
					++$REQS;
					$reply->(405,'{}',1);
				}
				# $rbuf = substr($rbuf);
			}
			elsif ($ret == -2) {
				warn "Incomplete";
				return $rw = AE::io $fh, 0, $rwcb;
			}
			else {
				warn "Broken request";
				return $fin->();
			}
		}
		elsif (defined $r) { # connection closed
			return $fin->();
		}
		elsif ($! == Errno::EAGAIN) {
			warn "$!";
			$rw = AE::io $fh, 0, $rwcb;
			return;
		}
		else {
			warn "$!";
			return $fin->();
		}

	};$rwcb->();

}, sub {
	my $fh = shift;
	setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
		or warn "setsockopt TCP_NODELAY: $!";
	# setsockopt($fh, IPPROTO_TCP, TCP_DEFER_ACCEPT, 1)
	# 	or warn "setsockopt TCP_DEFER_ACCEPT: $!";
	setsockopt($fh, IPPROTO_TCP, TCP_CORK, 0)
		or warn "setsockopt TCP_CORK: $!";
	if (defined &TCP_FASTOPEN) {
		setsockopt($fh, IPPROTO_TCP, TCP_FASTOPEN(), 10)
			or warn "setsockopt TCP_FASTOPEN: $!";
	}
	if (defined &TCP_QUICKACK) {
		setsockopt($fh, IPPROTO_TCP, TCP_QUICKACK(), 1)
			or warn "setsockopt TCP_QUICKACK: $!";
	}
	if (defined &TCP_LINGER2) {
		setsockopt($fh, IPPROTO_TCP, TCP_LINGER2(), 0)
			or warn "setsockopt TCP_LINGER2: $!";
	}
	2048
};

my $s = EV::signal TERM => sub {
	warn "Stop";
	EV::unloop;
};
EV::loop;
exit;


__END__
tcp_server 0, $port, sub {
	my $fh = shift;
	# AnyEvent::Util::fh_nonblocking($fh,1);
	++$CONNS;
	setsockopt($fh, SOL_SOCKET, SO_LINGER, 0);
	binmode ($fh, ':raw');
	my $rbuf;
	my $cl;
	my $rw;
	my ($m,$cap,$met,$path_query,$path,$qr);
	my $start = time;
	my $read_time;
	my $read_count = 0;
	my $rwcb;
	my $reply = sub {
		my ($st,$b) = @_;
		my $run = time - $start;
		my $N = $NAMES{ 0+$m } // 'OTH';
		$STAT{'C'.$N}++;
		$STAT{'R'.$N}+= $run;

		warn "$met $path $st: $b\n" if DEBUG > 1; # or $cap && $cap->{id} == 123;
		my $wbuf = "HTTP/1.1 $st XXX\015\012Server: Perl/5\015\012Connection: close\015\012Content-Length: ".
			length($b)."\015\012\015\012".$b;
		my $wr = syswrite($fh,$wbuf);
		if ($wr == length $wbuf) {
			if ($met eq 'GET') {
				return $rwcb->();
			}
		}
		else {
			warn "Write failed: $!\n";
		}
		close $fh;
		undef $rw;
		undef $rwcb;
		--$CONNS;
		--$REQS;
	};
	$rwcb = sub {
		# warn "Readable";
		my $r = sysread($fh, $rbuf, 256*1024, length $rbuf);
		$read_count++;
		if ($r) {
			($met,$path_query) = $rbuf =~ m{^([^ ]+)\s+([^ ]+)\s+[^\012]*\012}gc or return;
			($path,$qr) = split '\?', $path_query, 2;
			if ($met eq 'GET') {
				$read_time = time - $start;
				# ($m,$cap) = $GET->match($path);
				++$REQS;
				# $m or return $reply->(404, '{"error":"path"}');
				# my $query = decode_query($qr);
				# $m->( $reply, $cap->{id}, $query );
				if ($rbuf =~ m{\015\012\015\012}gc) {

				}
				else {

				}
				$reply->(200,'{}');
			}
			elsif ($met eq 'POST') {
				$reply->(200,'{}');
				# ($m,$cap) = $POST->match($path);
				# $m or return ++$REQS, $reply->(404, '{"error":"path"}');
				# if( ($cl) = $rbuf =~ /Content-Length:\s*(\d+)/gci ) { $rbuf =~ m{\015\012\015\012}gc or return; }
				# elsif ($rbuf =~ m{\015\012\015\012}gc) {
				# 	# $cl = 0;
				# 	++$REQS;
				# 	return $reply->(400, '{"error":"empty post"}');
				# }
				# else {return;}
				# my $end = pos $rbuf;
				# # my $end = index($rbuf,"\015\012\015\012", pos $rbuf);
				# # return if $end == -1;
				# return if length($rbuf) < $end + $cl;
				# $read_time = time - $start;
				# # p $rbuf;
				# # warn length($rbuf), " ", $end ," ", $cl;
				# # use Data::Dumper;
				# # warn Dumper [$rbuf] if $rbuf =~ /:\s*123b/;
				# my $data;
				# # p $rbuf;
				# # p substr($rbuf,$end);
				# # p substr($rbuf,$end,$cl);
				# ++$REQS;
				# eval {
				# 	$data = $JSON->decode(substr($rbuf,$end,$cl));
				# 1} or do {
				# 	# if ($cap->{id} == 123) {
				# 		# warn "Bad: '".substr($rbuf,$end,$cl)."'\n";
				# 	# }
				# 	return $reply->(400, '{"error":"bad json"}');
				# };
				# my $query = decode_query($qr);
				# $m->( $reply, $cap->{id}, $query, $data );
			}
			else {
				++$REQS;
				$reply->(503, "{}");
			}
			return;
		}
		elsif (defined $r) {
			++$REQS;
			$reply->(501, "{}");
		}
		elsif ($! == Errno::EAGAIN) {
			warn "EAGAIN";
			$rw = AE::io $fh,0, $rwcb;
			return;
		}
		else {
			undef $rwcb;
			undef $rw;
			close $fh;
			--$CONNS;
			return;
		}
	};
	$rwcb->();
}, sub {
	my $fh = shift;
	# TCP_DEFER_ACCEPT?
	setsockopt($fh, IPPROTO_TCP, TCP_NODELAY, 1)
		or warn "setsockopt TCP_NODELAY: $!";
	if (defined &TCP_FASTOPEN) {
		setsockopt($fh, IPPROTO_TCP, TCP_FASTOPEN(), 10)
			or warn "setsockopt TCP_FASTOPEN: $!";
	}
	if (defined &TCP_QUICKACK) {
		setsockopt($fh, IPPROTO_TCP, TCP_QUICKACK(), 1)
			or warn "setsockopt TCP_QUICKACK: $!";
	}
	if (defined &TCP_LINGER2) {
		setsockopt($fh, IPPROTO_TCP, TCP_LINGER2(), 0)
			or warn "setsockopt TCP_LINGER2: $!";
	}
	# setsockopt(sd, SOL_SOCKET, SO_RCVLOWAT, 10)
	# 	or warn "setsockopt SO_RCVLOWAT: $!";

	1024
};

my $s = EV::signal TERM => sub {
	warn "Stop";
	EV::unloop;
};
EV::loop;
exit;








