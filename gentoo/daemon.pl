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
use Local::HLCup;
use Local::HTTPServer;

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

Local::HLCup::mlockall();

BEGIN {
	eval { Socket->import('TCP_QUICKACK');1} or warn "No TCP_QUICKACK\n";
	eval { Socket->import('TCP_LINGER2');1}  or warn "No TCP_LINGER2\n";
	eval { Socket->import('TCP_CORK');1}  or warn "No TCP_CORK\n";
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

sub get_location_avg {
	my ($id,$prm) = @_;

	my $from = 0;
	my $till = 2**31-1;
	my $from_age = 2**31-1;
	my $till_age = -2**31;
	my $gender;

	if (exists $prm->{fromDate}) {
		return 400,'{}' unless $prm->{fromDate} =~ /^\d+$/;
		$from = $prm->{fromDate};
	}
	if (exists $prm->{toDate}) {
		return 400,'{}' unless $prm->{toDate} =~ /^\d+$/;
		$till = $prm->{toDate};
	}

	if (exists $prm->{fromAge}) {
		return 400,'{}' unless $prm->{fromAge} =~ /^\d+$/;
		$from_age = $NOW->minus_years( $prm->{fromAge} )->epoch;
	}
	if (exists $prm->{toAge}) {
		return 400,'{}' unless $prm->{toAge} =~ /^\d+$/;
		$till_age = $NOW->minus_years( $prm->{toAge} )->epoch;
	}
	if (exists $prm->{gender}) {
		return 400,'{}' unless $prm->{gender} =~ /^(f|m)$/;
		$gender = $prm->{gender};
	}

	return $db->get_location_avg($id,$from,$till,$till_age,$from_age,$gender);
}

sub get_location_visits {
	my ($id,$prm) = @_;

	my $from = 0;
	my $till = 2**31-1;
	my $from_age = 2**31-1;
	my $till_age = -2**31;
	my $gender;

	if (exists $prm->{fromDate}) {
		return 400,'{}' unless $prm->{fromDate} =~ /^\d+$/;
		$from = $prm->{fromDate};
	}
	if (exists $prm->{toDate}) {
		return 400,'{}' unless $prm->{toDate} =~ /^\d+$/;
		$till = $prm->{toDate};
	}

	if (exists $prm->{fromAge}) {
		return 400,'{}' unless $prm->{fromAge} =~ /^\d+$/;
		$from_age = $NOW->minus_years( $prm->{fromAge} )->epoch;
	}
	if (exists $prm->{toAge}) {
		return 400,'{}' unless $prm->{toAge} =~ /^\d+$/;
		$till_age = $NOW->minus_years( $prm->{toAge} )->epoch;
	}
	if (exists $prm->{gender}) {
		return 400,'{}' unless $prm->{gender} =~ /^(f|m)$/;
		$gender = $prm->{gender};
	}

	my $rv = $db->get_location_visits($id,$from,$till,$till_age,$from_age,$gender)
		or return 404,'{}';
	return 200, JSON::XS->new->utf8->pretty->encode($rv);

}

sub update_user {
	my ($id,$prm,$data) = @_;
	eval { $data = $JSON->decode($data);1} or return 400, '{"error":"bad json"}';

	return 404,'{}' unless $db->exists_user($id);

	length $data->{email} and length $data->{email} < 100
		or return 400,'{"error":"bad email"}'
		if exists $data->{email};
	$data->{birth_date} =~ /^-?\d+$/
		or return 400,'{"error":"bad birth_date"}'
		if exists $data->{birth_date};
	$data->{gender} =~ /^(f|m)$/
		or return 400,'{"error":"bad gender"}'
		if exists $data->{gender};
	length $data->{first_name} and length $data->{first_name} < 50
		or return 400,'{"error":"bad first_name"}'
		if exists $data->{first_name};
	length $data->{last_name} and length $data->{last_name} < 50
		or return 400,'{"error":"bad last_name"}'
		if exists $data->{last_name};

	AE::postpone {
		$db->update_user($id, @{ $data }{qw( email first_name last_name gender birth_date)});
	};
	return "200",'{}';
}

sub update_location {
	my ($id,$prm,$data) = @_;
	eval { $data = $JSON->decode($data);1} or return 400, '{"error":"bad json"}';
	return 404,'{"error":"not exists"}' unless $db->exists_location($id);

	length $data->{place}
		or return 400,'{"error":"bad place"}'
		if exists $data->{place};

	length $data->{city} and length $data->{city} < 50
		or return 400,'{"error":"bad city"}'
		if exists $data->{city};

	$data->{distance} =~ /^\d+$/
		or return 400,'{"error":"bad distance"}'
		if exists $data->{distance};

	if (exists $data->{country}) {
		length $data->{country} and length $data->{country} < 50
			or return 400,'{"error":"bad country"}'
			;
		$data->{country} = $db->get_country($data->{country});
	}
	AE::postpone {
		$db->update_location($id, @{$data}{qw(country distance city place) });
	};
	return 200,'{}';
}

sub update_visit {
	my ($id,$prm,$data) = @_;
	eval { $data = $JSON->decode($data);1} or return 400, '{"error":"bad json"}';
	return 404,'{}' unless $db->exists_visit($id);

	$data->{mark} =~ /^[0-5]$/
		or return 400,'{"error":"bad mark"}'
		if exists $data->{mark};

	if (exists $data->{visited_at}) {
		$data->{visited_at} =~ /^\d+$/
			or return 400,'{"error":"bad visited_at"}'
	}

	if (exists $data->{location}) {
		return 400,'{"error":"bad location"}'
			if $data->{location} !~ /^\d+$/ or !$db->exists_location($data->{location});
	}
	if (exists $data->{user}) {
		return 400,'{"error":"bad user"}'
			if $data->{user} !~ /^\d+$/ or !$db->exists_user($data->{user});
	}
	AE::postpone {
		$db->update_visit($id, $data->{user}, $data->{location}, $data->{mark} // -1, $data->{visited_at} );
	};
	return 200,'{}';
}

sub create_user {
	my (undef,$prm,$data) = @_;
	eval { $data = $JSON->decode($data);1} or return 400, '{"error":"bad json"}';
	if ($data->{id} !~ /^\d+$/ or $db->exists_user($data->{id})) {
		return 400,'{"error":"bad id"}';
	}

	length $data->{email} and length $data->{email} < 100
		or return 400,'{"error":"bad email"}'
		;
	$data->{birth_date} =~ /^-?\d+$/
		or return 400,'{"error":"bad birth_date"}'
		;
	$data->{gender} =~ /^(f|m)$/
		or return 400,'{"error":"bad gender"}'
		;
	length $data->{first_name} and length $data->{first_name} < 50
		or return 400,'{"error":"bad first_name"}'
		;
	length $data->{last_name} and length $data->{last_name} < 50
		or return 400,'{"error":"bad last_name"}'
		;
	AE::postpone {
		$db->add_user(@{ $data }{qw( id email first_name last_name gender birth_date)});
	};
	return "200",'{}';
}

sub create_location {
	my (undef,$prm,$data) = @_;
	eval { $data = $JSON->decode($data);1} or return 400, '{"error":"bad json"}';
	if ($data->{id} !~ /^\d+$/ or $db->exists_location($data->{id})) {
		return 400,q|{"error":"bad id"}|;
	}

	length $data->{place}
		or return 400,'{"error":"bad place"}'
		;

	length $data->{country} and length $data->{country} < 50
		or return 400,'{"error":"bad country"}'
		;

	length $data->{city} and length $data->{city} < 50
		or return 400,'{"error":"bad city"}'
		;

	$data->{distance} =~ /^\d+$/
		or return 400,'{"error":"bad distance"}'
		;
	AE::postpone {
		$db->add_location($data->{id},$db->get_country($data->{country}), @{$data}{ qw(distance city place) });
	};
	return "200",'{}';
}

sub create_visit {
	my (undef,$prm,$data) = @_;
	eval { $data = $JSON->decode($data);1} or return 400, '{"error":"bad json"}';
	if ($data->{id} !~ /^\d+$/ or $db->exists_visit($data->{id})) {
		return 400,'{"error":"bad id"}';
	}
	if (!$db->exists_user($data->{user})) {
		return 400,'{"error":"bad user"}';
	}
	if (!$db->exists_location($data->{location})) {
		return 400,'{"error":"bad location"}';
	}
	$data->{mark} =~ /^[0-5]$/ or return 400,'{"error":"bad mark"}';
	$data->{visited_at} =~ /^\d+$/ or return 400,'{"error":"bad visited_at"}';
	AE::postpone {
		$db->add_visit(@{$data}{qw(id user location mark visited_at)});
	};
	return 200,'{}';
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
my $s = EV::signal TERM => sub {
	warn "Stop";
	EV::unloop;
};
DB::enable_profile() if defined &DB::enable_profile;


my $server = Local::HTTPServer->new("0.0.0.0",$port, [
	sub { return 501,"Fuck off"; },
	\&create_user,
	\&Local::HLCup::get_user_visits_rv,
	\&Local::HLCup::get_user_rv,
	\&update_user,

	\&create_location,
	\&get_location_avg,
	\&get_location_visits,
	\&Local::HLCup::get_location_rv,
	\&update_location,

	\&create_visit,
	\&Local::HLCup::get_visit_rv,
	\&update_visit,
]);

$server->listen();
$server->accept();

my $s = EV::signal TERM => sub {
	warn "Stop";
	EV::unloop;
};
EV::loop;
exit;








