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
use Local::HLCup;
use HTTP::Parser::XS qw(parse_http_request);

##################################################


my $port;
my $debug;
my $src;
my $options;
our $DST;
our $NOW;

my $mode_local_train;
my $mode_run_test;
my $mode_run_prod;

BEGIN {
	$options = '/tmp/data/options.txt';
	$DST = '/tmp/unpacked';
	$port = 80;
	$src = 'TRAIN';
	GetOptions(
		'p|port=n' => \$port,
		'd|debug+' => \$debug,
		's|source=s' => \$src,
		'o|options=s' => \$options,
	) or die;
	if ($debug) {
		$DST = "hlcupdocs/data/$src/data";
		$options = "hlcupdocs/data/$src/data/options.txt";
		$mode_local_train = 1 if $src eq 'TRAIN';
	}
	my $mode;
	if (open my $f, $options) {
		$NOW = 0+<$f>;
		$mode = 0+<$f>;
		close $f;
		warn "Loaded time = $NOW, mode = $mode from $options\n";
	}
	else {
		warn "$options: $!\n";
	}
	$mode_run_test = $mode == 0;
	$mode_run_prod = $mode == 1;
}
use constant DEBUG => $debug;
use constant LOCAL_TRAIN => $mode_local_train;
use constant RUN_TEST => $mode_run_test;
use constant RUN_PROD => $mode_run_prod;

our $JSON = JSON::XS->new->utf8;
our $JSONN = JSON::XS->new->utf8->allow_nonref;

warn `cat /proc/cpuinfo | egrep 'model name|bogomips' | head -n2`;
system("ulimit -n 200000");

system("sysctl net.ipv4.tcp_fin_timeout");
system("sysctl net.ipv4.tcp_tw_reuse");
system("sysctl net.ipv4.tcp_tw_recycle");

system("sysctl net.ipv4.tcp_tw_reuse=1");
system("sysctl net.ipv4.tcp_tw_recycle=1");
system("sysctl net.ipv4.tcp_slow_start_after_idle=0");

BEGIN {
	eval { Socket->import('TCP_QUICKACK');1} or warn "No TCP_QUICKACK\n";
	eval { Socket->import('TCP_LINGER2');1}  or warn "No TCP_LINGER2\n";
}
unless (DEBUG) {
	my $start = time;
	system("unzip -o /tmp/data/data.zip -d $DST/ >/dev/null 2>/dev/null")
		== 0 or die "Failed to unpack: $?";
	warn sprintf "Unpacked archive in %0.4fs\n", time - $start;
}
##################################################

sub aefor($$$$;$);

my @targets = qw(logger worker  ); # heater monitor
my %chld;
END{ kill KILL => $_ for keys %chld };
my $logger;

my $log;
pipe my $logr,my $logw or die "pipe: $!";

for my $target (@targets) {
	defined(my $pid = fork) or die "$!";
	if ($pid) {
		if ($target eq 'logger') {
			close $logr;
			AnyEvent::Util::fh_nonblocking($logw,1);
			$SIG{__WARN__} = sub {
				my $now = Time::Moment->now;
				syswrite $logw, sprintf("[%s][%s] %s", $now->strftime("%H:%M:%S%3f"),$$,@_);
			};
			$logger = $pid;
		}
		else {
			$chld{$pid} = $target;
		}
		warn "Spawned $pid as $target\n";
	}
	else {
		%chld = ();
		$SIG{INT} = 'IGNORE';
		$0 = $target;
		no strict 'refs';
		$target->();
		exit;
	}
}

my $working = 1;
sub terminus {
	# delete $SIG{__WARN__};
	warn "killing";
	kill TERM => $_ for keys %chld;
	$working = 0;
}

$SIG{CHLD} = sub {
	while ((my $child = waitpid(-1,WNOHANG)) > 0) {
		my ($exitcode, $signal, $core) = ($? >> 8, $? & 127, $? & 128);
		unless(kill 0 => $child) {
			warn "Child $child gone: $exitcode\n";
			delete $chld{$child};
		}
	}
};

$SIG{TERM} = \&terminus;
$SIG{INT}  = \&terminus;
$SIG{QUIT} = \&terminus;

while ($working) {sleep 0.1;}
my $wait = 10;
while ($wait-- > 0 and %chld ) {sleep 0.1;}
kill TERM => $logger;
warn "gone all ($wait)\n";
exit;

sub logger {
	close $logw;
	while (<$logr>) { print STDERR $_;}
	exit;
}

sub monitor {
	warn "I'm monitor";
	my $work = 1;
	$SIG{TERM} = sub {$work = 0};
	while ($work) { sleep 1; }
}

sub heater {
	warn "I'm heater";
	sleep 3;
	warn "Do work...";
	exit;
}

sub worker {
	goto WORKER;
}
WORKER:

my $db = Local::HLCup->new();

# $SIG{__DIE__} = sub {
# 	warn "@_";
# };
$EV::DIED = sub {
	warn "@_/$@";
};

# our @USERS;
# $#USERS = 110000;
# our %COUNTRIES; our $COUNTRY_MAX=0; # by name
# keys (%COUNTRIES) = 200;
# our @COUNTRY_ID;
# $#COUNTRY_ID = 200;
# our @LOCATIONS;
# $#LOCATIONS = 110000;
# our @VISITS;
# $#VISITS = 1100000;
# our @USER_VISITS;
# $#USER_VISITS = 110000;
# our @LOCATION_VISITS;
# $#LOCATION_VISITS = 110000;
our %STAT;

# sub get_country($) {
# 	my $key = shift;
# 	my $country = $COUNTRIES{ $key } ||= do {
# 		my $id = ++$COUNTRY_MAX;
# 		$COUNTRY_ID[$id] = { id => $id, name => $key };
# 	};
# 	return $country->{id};
# }


################ Loading DATA
{
	my ($start,$count);
	warn "Loading DATA from $DST\n";
	$start = time; $count = 0;
	for my $f (<$DST/users_*.json>) {
		$NOW //= (stat($f))[9];
		my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{users}};
		for my $u (@$data) {
			$db->add_user(@{ $u }{qw( id email first_name last_name gender birth_date)});
			++$count;
		}
	}
	warn sprintf "Loaded %d users in %0.4fs\n", $count, time-$start;

	$start = time; $count = 0;
	for my $f (<$DST/locations_*.json>) {
		my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{locations}};
		for my $loc (@$data) {
			$loc->{country} = $db->get_country($loc->{country});
			++$count;
			$db->add_location($loc->{id},$db->get_country($loc->{country}), @{$loc}{ qw(distance city place) });
		}
	}
	warn sprintf "Loaded %d locations in %0.4fs\n", $count, time-$start;

	$start = time; $count = 0;
	for my $f (<$DST/visits_*.json>) {
		my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{visits}};
		for my $vis (@$data) {
			$db->add_visit(@{$vis}{qw(id user location mark visited_at)});
			++$count;
		}
	}
	warn sprintf "Loaded %d visits in %0.4fs\n", $count, time-$start;
	$NOW //= time;
}
warn mystat(),"\n";
AE::now_update();
$NOW = Time::Moment->from_epoch($NOW);
################ Loaded DATA
sub get_user {
	my ($res,$id) = @_;
	my $user = $db->get_user($id) or return $res->(404,'{}');
	return $res->(200, $user);
}

sub get_visit {
	my ($res,$id) = @_;
	my $visit = $db->get_visit($id) or return $res->(404,'{}');
	return $res->(200, $visit);
}

sub get_location {
	my ($res,$id) = @_;
	my $loc = $db->get_location($id) or return $res->(404,'{}');
	return $res->(200, $loc);
}

sub get_user_visits {
	my ($res,$id,$prm) = @_;

	my $from = 0;
	my $till = 2**31-1;
	my $country;
	my $distance;

	if (exists $prm->{fromDate}) {
		return $res->(400,'{}') unless $prm->{fromDate} =~ /^\d+$/;
		$from = $prm->{fromDate};
	}
	if (exists $prm->{toDate}) {
		return $res->(400,'{}') unless $prm->{toDate} =~ /^\d+$/;
		$till = $prm->{toDate};
	}
	if (exists $prm->{country}) {
		$country = $db->get_country($prm->{country});
		return $res->(400,'{"error":"Bad country"}') unless $country;
	}
	if (exists $prm->{toDistance}) {
		return $res->(400,'{}') unless $prm->{toDistance} =~ /^\d+$/;
		$distance = $prm->{toDistance};
	}
	my $vis = $db->get_user_visits($id,$from,$till,$country,$distance)
		or return $res->(404,'{}');
	
	return $res->(200, $vis);
}

sub get_location_avg {
	my ($res,$id,$prm) = @_;

	my $from = 0;
	my $till = 2**31-1;
	my $from_age = 2**31-1;
	my $till_age = -2**31;
	my $gender;

	if (exists $prm->{fromDate}) {
		return $res->(400,'{}') unless $prm->{fromDate} =~ /^\d+$/;
		$from = $prm->{fromDate};
	}
	if (exists $prm->{toDate}) {
		return $res->(400,'{}') unless $prm->{toDate} =~ /^\d+$/;
		$till = $prm->{toDate};
	}

	if (exists $prm->{fromAge}) {
		return $res->(400,'{}') unless $prm->{fromAge} =~ /^\d+$/;
		$from_age = $NOW->minus_years( $prm->{fromAge} )->epoch;
	}
	if (exists $prm->{toAge}) {
		return $res->(400,'{}') unless $prm->{toAge} =~ /^\d+$/;
		$till_age = $NOW->minus_years( $prm->{toAge} )->epoch;
	}
	if (exists $prm->{gender}) {
		return $res->(400,'{}') unless $prm->{gender} =~ /^(f|m)$/;
		$gender = $prm->{gender};
	}

	my $rv = $db->get_location_avg($id,$from,$till,$till_age,$from_age,$gender)
		or return $res->(404,'{}');
	return $res->(200, $rv);
}

sub get_location_visits {
	my ($res,$id,$prm) = @_;

	my $from = 0;
	my $till = 2**31-1;
	my $from_age = 2**31-1;
	my $till_age = -2**31;
	my $gender;

	if (exists $prm->{fromDate}) {
		return $res->(400,'{}') unless $prm->{fromDate} =~ /^\d+$/;
		$from = $prm->{fromDate};
	}
	if (exists $prm->{toDate}) {
		return $res->(400,'{}') unless $prm->{toDate} =~ /^\d+$/;
		$till = $prm->{toDate};
	}

	if (exists $prm->{fromAge}) {
		return $res->(400,'{}') unless $prm->{fromAge} =~ /^\d+$/;
		$from_age = $NOW->minus_years( $prm->{fromAge} )->epoch;
	}
	if (exists $prm->{toAge}) {
		return $res->(400,'{}') unless $prm->{toAge} =~ /^\d+$/;
		$till_age = $NOW->minus_years( $prm->{toAge} )->epoch;
	}
	if (exists $prm->{gender}) {
		return $res->(400,'{}') unless $prm->{gender} =~ /^(f|m)$/;
		$gender = $prm->{gender};
	}

	my $rv = $db->get_location_visits($id,$from,$till,$till_age,$from_age,$gender)
		or return $res->(404,'{}');
	return $res->(200, JSON::XS->new->utf8->pretty->encode($rv));

}

sub update_user {
	my ($res,$id,$prm,$data) = @_;
	return $res->(404,'{}') unless $db->exists_user($id);

	length $data->{email} and length $data->{email} < 100
		or return $res->(400,'{"error":"bad email"}')
		if exists $data->{email};
	$data->{birth_date} =~ /^-?\d+$/
		or return $res->(400,'{"error":"bad birth_date"}')
		if exists $data->{birth_date};
	$data->{gender} =~ /^(f|m)$/
		or return $res->(400,'{"error":"bad gender"}')
		if exists $data->{gender};
	length $data->{first_name} and length $data->{first_name} < 50
		or return $res->(400,'{"error":"bad first_name"}')
		if exists $data->{first_name};
	length $data->{last_name} and length $data->{last_name} < 50
		or return $res->(400,'{"error":"bad last_name"}')
		if exists $data->{last_name};

	$res->("200",'{}');

	$db->update_user($id, @{ $data }{qw( email first_name last_name gender birth_date)});
}

sub update_location {
	my ($res,$id,$prm,$data) = @_;
	return $res->(404,'{}') unless $db->exists_location($id);

	length $data->{place}
		or return $res->(400,'{"error":"bad place"}')
		if exists $data->{place};

	length $data->{city} and length $data->{city} < 50
		or return $res->(400,'{"error":"bad city"}')
		if exists $data->{city};

	$data->{distance} =~ /^\d+$/
		or return $res->(400,'{"error":"bad distance"}')
		if exists $data->{distance};

	if (exists $data->{country}) {
		length $data->{country} and length $data->{country} < 50
			or return $res->(400,'{"error":"bad country"}');
		$data->{country} = $db->get_country($data->{country});
	}

	$res->("200",'{}');

	$db->update_location($id, @{$data}{qw(country distance city place) });
}

sub update_visit {
	my ($res,$id,$prm,$data) = @_;
	return $res->(404,'{}') unless $db->exists_visit($id);

	$data->{mark} =~ /^[0-5]$/
		or return $res->(400,'{"error":"bad mark"}')
		if exists $data->{mark};

	if (exists $data->{visited_at}) {
		$data->{visited_at} =~ /^\d+$/
			or return $res->(400,'{"error":"bad visited_at"}');
	}

	if (exists $data->{location}) {
		return $res->(400,'{"error":"bad location"}')
			if $data->{location} !~ /^\d+$/ or !$db->exists_location($data->{location});
	}
	if (exists $data->{user}) {
		return $res->(400,'{"error":"bad user"}')
			if $data->{user} !~ /^\d+$/ or !$db->exists_user($data->{user});
	}
	$res->(200,'{}');

	$db->update_visit($id, $data->{user}, $data->{location}, $data->{mark} // -1, $data->{visited_at} );
}

sub create_user {
	my ($res,undef,$prm,$data) = @_;
	if ($data->{id} !~ /^\d+$/ or $db->exists_user($data->{id})) {
		return $res->(400,'{"error":"bad id"}');
	}

	length $data->{email} and length $data->{email} < 100
		or return $res->(400,'{"error":"bad email"}')
		;
	$data->{birth_date} =~ /^-?\d+$/
		or return $res->(400,'{"error":"bad birth_date"}')
		;
	$data->{gender} =~ /^(f|m)$/
		or return $res->(400,'{"error":"bad gender"}')
		;
	length $data->{first_name} and length $data->{first_name} < 50
		or return $res->(400,'{"error":"bad first_name"}')
		;
	length $data->{last_name} and length $data->{last_name} < 50
		or return $res->(400,'{"error":"bad last_name"}')
		;

	$res->("200",'{}');
	
	$db->add_user(@{ $data }{qw( id email first_name last_name gender birth_date)});
}

sub create_location {
	my ($res,undef,$prm,$data) = @_;
	if ($data->{id} !~ /^\d+$/ or $db->exists_location($data->{id})) {
		return $res->(400,qq|{"error":"bad id: $data->{id}"}|);
	}

	length $data->{place}
		or return $res->(400,'{"error":"bad place"}')
		;

	length $data->{country} and length $data->{country} < 50
		or return $res->(400,'{"error":"bad country"}')
		;

	length $data->{city} and length $data->{city} < 50
		or return $res->(400,'{"error":"bad city"}')
		;

	$data->{distance} =~ /^\d+$/
		or return $res->(400,'{"error":"bad distance"}')
		;
	$res->("200",'{}');

	$db->add_location($data->{id},$db->get_country($data->{country}), @{$data}{ qw(distance city place) });
	return;
}

sub create_visit {
	my ($res,undef,$prm,$data) = @_;
	if ($data->{id} !~ /^\d+$/ or $db->exists_visit($data->{id})) {
		return $res->(400,'{"error":"bad id"}');
	}
	if (!$db->exists_user($data->{user})) {
		return $res->(400,'{"error":"bad user"}');
	}
	if (!$db->exists_location($data->{location})) {
		return $res->(400,'{"error":"bad location"}');
	}
	$data->{mark} =~ /^[0-5]$/ or return $res->(400,'{"error":"bad mark"}');
	$data->{visited_at} =~ /^\d+$/ or return $res->(400,'{"error":"bad visited_at"}');

	$res->(200,'{}');

	$db->add_visit(@{$data}{qw(id user location mark visited_at)});
}

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

use AnyEvent::Socket;

my $min = 1e10;
my $sum = 0;
my $max = 0;
my $cnt = 0;
my $prev = $cnt;
my $last_ac = 'same';

sub mystat {
	# return $JSON->encode(\%STAT);
    my ($vsize,$rss) = (0,0);
    eval {
            my $stat = do { open my $f,'<:raw',"/proc/$$/stat" or die $!; local $/; <$f> };
            ($vsize,$rss) = $stat =~ m{ ^ \d+ \s+ \(.+?\) \s+ [RSDZTW] \s+ (?:\S+\s+){19} (\S+) \s+ (\S+) \s+}xs;
    };
    my $now = time;
    return sprintf "%+0.3f : %0.4fs+%0.4fs : %0.2fM/%0.2fM : %s",
            $now-AE::now(), (times)[0,1],
            $rss/(1024*1024/4096), $vsize/(1024*1024),
            $JSON->encode(\%STAT);
}

my $CONNS = my $REQS = 0;

sub do_stat {
	sprintf "[$CONNS:$REQS] %+d, Cnt: %d; Min: %0.4fms; Max: %0.4fms; Avg: %0.4fms [%s]\n",
		$cnt-$prev, $cnt, $min*1000, $max*1000, eval{$sum*1000/$cnt}, mystat();
}

# my $g;$g = EV::timer 0,10, sub {
# 	if ($prev == $cnt) {
# 		# warn "Same";
# 		if ($last_ac eq 'grow') {
# 			# warn "Stopped";
# 			warn sprintf "END[$CONNS:$REQS]; Cnt: %d; Min: %0.4fms; Max: %0.4fms; Avg: %0.4fms [%s]\n", $cnt, $min*1000, $max*1000, eval{$sum*1000/$cnt}, mystat();
# 			$prev = $cnt = $max = $sum = 0;
# 			$min = 1e10;
# 		}
# 		$last_ac = 'same';
# 	}
# 	else {
# 		# if ($last_ac eq 'same') {
# 		# 	warn "Start\n";
# 		# }
# 		warn sprintf "Grow[$CONNS:$REQS]: %+d, Cnt: %d; Min: %0.4fms; Max: %0.4fms; Avg: %0.4fms [%s]\n", $cnt-$prev, $cnt, $min*1000, $max*1000, eval{$sum*1000/$cnt}, mystat();
# 		$prev = $cnt;
# 		$last_ac = 'grow';
# 	}
# };


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
		if (LOCAL_TRAIN) {
			if ($cnt == 3000 or $cnt == 3000+3000 or $cnt == 3000+3000+5000) {
				warn "STAT $cnt ".do_stat();
			}
		}
		if (RUN_TEST) {
			if ($cnt == 9030 or $cnt == 9030+3000 or $cnt == 9030+3000+19500) {
				warn "STAT $cnt ".do_stat();
			}
		}
		if (RUN_PROD) {
			if ($cnt == 150150 or $cnt == 150150+40000 or $cnt == 150150+40000+630000) {
				warn "STAT $cnt ".do_stat();
			}
		}


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
					$m->( $reply, $cap->{id}, $query );
					# $reply->(400,'{}',0);
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
					$m->( $reply, $cap->{id}, $query, $data );
					# $reply->(400,'{}',1);
				}
				else {
					++$REQS;
					$reply->(405,'{}',1);
				}
				# $rbuf = substr($rbuf);
			}
			elsif ($ret == -2) {
				# warn "Incomplete";
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
			# warn "$!";
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
	setsockopt($fh,SOL_SOCKET, SO_LINGER, "\0\0\0\0");
	binmode ($fh, ':raw');
	my $rbuf;
	my $cl;
	my $rw;
	my ($m,$cap,$met,$path_query,$path,$qr);
	my $start = time;
	my $read_time;
	my $read_count = 0;
	my $reply = sub {
		my ($st,$b) = @_;
		my $run = time - $start;
		my $N = $NAMES{ 0+$m } // 'OTH';
		$STAT{'C'.$N}++;
		$STAT{'R'.$N}+= $run;

		$min = min($min,$run);
		$max = max($max,$run);
		$sum += $run;
		$cnt++;
		if ( $run > 0.5 ) {
			warn sprintf "[%s:%s] %s %s %s %0.4fs (%0.4fs,%s) [%s]\n",$CONNS, $REQS, $met,$path_query,$st,$run,$read_time,$read_count, $JSONN->encode($rbuf);
		}
		warn "$met $path $st: $b\n" if DEBUG > 1; # or $cap && $cap->{id} == 123;
		my $wbuf = "HTTP/1.1 $st XXX\015\012Server: Perl/5\015\012Connection: close\015\012Content-Length: ".
			length($b)."\015\012\015\012".$b;
		my $wr = syswrite($fh,$wbuf);
		if ($wr == length $wbuf) {

		}
		else {
			warn "Write failed: $!\n";
		}
		close $fh;
		undef $rw;
		--$CONNS;
		--$REQS;
	};
	$rw = AE::io $fh,0, sub {
		my $r = sysread($fh, $rbuf, 256*1024, length $rbuf);
		$read_count++;
		if ($r) {
			($met,$path_query) = $rbuf =~ m{^([^ ]+)\s+([^ ]+)\s+[^\012]*\012}gc or return;
			($path,$qr) = split '\?', $path_query, 2;
			if ($met eq 'GET') {
				$read_time = time - $start;
				($m,$cap) = $GET->match($path);
				++$REQS;
				$m or return $reply->(404, '{"error":"path"}');
				my $query = decode_query($qr);
				$m->( $reply, $cap->{id}, $query );
			}
			elsif ($met eq 'POST') {
				($m,$cap) = $POST->match($path);
				$m or return ++$REQS, $reply->(404, '{"error":"path"}');
				if( ($cl) = $rbuf =~ /Content-Length:\s*(\d+)/gci ) { $rbuf =~ m{\015\012\015\012}gc or return; }
				elsif ($rbuf =~ m{\015\012\015\012}gc) {
					# $cl = 0;
					++$REQS;
					return $reply->(400, '{"error":"empty post"}');
				}
				else {return;}
				my $end = pos $rbuf;
				# my $end = index($rbuf,"\015\012\015\012", pos $rbuf);
				# return if $end == -1;
				return if length($rbuf) < $end + $cl;
				$read_time = time - $start;
				# p $rbuf;
				# warn length($rbuf), " ", $end ," ", $cl;
				# use Data::Dumper;
				# warn Dumper [$rbuf] if $rbuf =~ /:\s*123b/;
				my $data;
				# p $rbuf;
				# p substr($rbuf,$end);
				# p substr($rbuf,$end,$cl);
				++$REQS;
				eval {
					$data = $JSON->decode(substr($rbuf,$end,$cl));
				1} or do {
					# if ($cap->{id} == 123) {
						# warn "Bad: '".substr($rbuf,$end,$cl)."'\n";
					# }
					return $reply->(400, '{"error":"bad json"}');
				};
				my $query = decode_query($qr);
				$m->( $reply, $cap->{id}, $query, $data );
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
			return;
		}
		else {
			undef $rw;
			close $fh;
			--$CONNS;
			return;
		}
	};
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


	1024
};

my $s = EV::signal TERM => sub {
	warn "Stop";
	EV::unloop;
};
EV::loop;
exit;








