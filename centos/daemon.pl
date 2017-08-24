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
use Socket qw(SOL_SOCKET SO_LINGER IPPROTO_TCP TCP_NODELAY);
use Router::R3;
use URI::XSEscape 'uri_unescape';

##################################################
my $port;
my $debug;
my $src;
BEGIN {
	$port = 80;
	$src = 'TRAIN';
	GetOptions(
		'p|port=n' => \$port,
		'd|debug+' => \$debug,
		's|source=s' => \$src,
	) or die;
}
use constant DEBUG => $debug;
our $JSON = JSON::XS->new->utf8;
our $JSONN = JSON::XS->new->utf8->allow_nonref;

system("ulimit -n 200000");
system("sysctl net.ipv4.tcp_tw_reuse=1");
system("sysctl net.ipv4.tcp_slow_start_after_idle=0");

BEGIN {
	eval { Socket->import('TCP_QUICKACK');1} or warn "No TCP_QUICKACK\n";
	eval { Socket->import('TCP_LINGER2');1}  or warn "No TCP_LINGER2\n";
}
our $DST = '/tmp/unpacked';
if (DEBUG) {
	$DST = "hlcupdocs/data/$src/data";
}
else {
	my $start = time;
	system("unzip -o /tmp/data/data.zip -d $DST/ >/dev/null 2>/dev/null")
		== 0 or die "Failed to unpack: $?";
	warn sprintf "Unpacked archive in %0.4fs\n", time - $start;
}
##################################################

sub aefor($$$$;$);

my @targets = qw(logger worker heater monitor);
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

sub aefor($$$$;$) {
	my $fin = pop;
	my $cb = pop;
	my ($from,$to,$steps) = @_;
	if ($to >= $from) {
		$steps //= 50;
		my $i = $from;
		my $step;$step = sub {
			local $_ = $i;
			my $iter_end = $i+$steps;
			$iter_end = $to if $iter_end > $to;
			while () {
				&$cb;
			}
			continue {
				$_++;
				if ($_ > $iter_end) {
					last if $_ > $to;
					$i = $_;
					&AE::postpone($step);
					return;
				}
			}
			undef $step;
			&$fin;
		};$step->();
	} else {
		$_ = $from;
		&$fin;
	}
}

our @USERS;
$#USERS = 110000;
our %COUNTRIES; our $COUNTRY_MAX=0; # by name
keys (%COUNTRIES) = 200;
our @COUNTRY_ID;
$#COUNTRY_ID = 200;
our @LOCATIONS;
$#LOCATIONS = 110000;
our @VISITS;
$#VISITS = 1100000;
our @USER_VISITS;
$#USER_VISITS = 110000;
our @LOCATION_VISITS;
$#LOCATION_VISITS = 110000;
our %STAT;

sub get_country($) {
	my $key = shift;
	my $country = $COUNTRIES{ $key } ||= do {
		my $id = ++$COUNTRY_MAX;
		$COUNTRY_ID[$id] = { id => $id, name => $key };
	};
	return $country->{id};
}


our $NOW;
################ Loading DATA
{
	my ($start,$count);
	warn "Loading DATA from $DST\n";
	$start = time; $count = 0;
	for my $f (<$DST/users_*.json>) {
		$NOW //= (stat($f))[9];
		my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{users}};
		# warn sprintf "Loading %d users from %s\n",0+@$data,$f;
		for my $u (@$data) {
			$USERS[$u->{id}] = $u;
			++$count;
		}
	}
	warn sprintf "Loaded %d users in %0.4fs\n", $count, time-$start;

	$start = time; $count = 0;
	for my $f (<$DST/locations_*.json>) {
		my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{locations}};
		# warn sprintf "Loading %d locations from %s\n",0+@$data,$f;
		for my $l (@$data) {
			# p $l unless %COUNTRIES;
			$l->{country} = get_country $l->{country};
			++$count;
			$LOCATIONS[$l->{id}] = $l;
		}
	}
	# p %COUNTRIES;
	warn sprintf "Loaded %d locations in %0.4fs, found %d countries\n", $count, time-$start, 0+keys %COUNTRIES;

	$start = time; $count = 0;
	for my $f (<$DST/visits_*.json>) {
		my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{visits}};
		# warn sprintf "Loading %d visits from %s\n",0+@$data,$f;
		for my $v (@$data) {
			my $loc = $LOCATIONS[$v->{location}];
			my $usr = $USERS[$v->{user}];
			$VISITS[$v->{id}] = $v;
			my $uvl = {
				user => $usr,
				visit => $v,
				location => $loc,
			};

			push @{ $USER_VISITS[ $v->{user} ] },         $uvl;
			push @{ $LOCATION_VISITS[ $v->{location} ] }, $uvl;

			++$count;
		}
	}
	warn sprintf "Loaded %d visits in %0.4fs\n", $count, time-$start;

	$start = time;
	my $max1 = 0;
	my $id1;
	for my $visits (@LOCATION_VISITS) {
		$visits or next;
		@$visits = sort {
			$a->{visit}{visited_at} <=> $b->{visit}{visited_at}
		} @$visits;
		if (0+@$visits > $max1) {
			$max1 = 0+@$visits;
			$id1 = $visits->[0]{location}{id};
		}
	}
	my $max2 = 0;
	my $id2;
	for my $visits (@USER_VISITS) {
		$visits or next;
		@$visits = sort {
			$a->{visit}{visited_at} <=> $b->{visit}{visited_at}
		} @$visits;
		if (0+@$visits > $max2) {
			$max2 = 0+@$visits;
			$id2 = $visits->[0]{location}{id};
		}
	}
	warn sprintf "Sorted %d visits in %0.4fs, max_loc: %s (%s), max_usr: %s (%s)\n", $count, time-$start, $max1, $id1, $max2, $id2;
	$NOW //= time;
}
$NOW = Time::Moment->from_epoch($NOW);
################ Loaded DATA

sub get_user {
	my ($res,$id,$q) = @_;
	my $user = $USERS[$id] or return $res->(404,'{}');
	return $res->(200, $JSON->encode({
		id           => 0+$user->{id},
		email        => $user->{email},
		gender       => $user->{gender},
		first_name   => $user->{first_name},
		last_name    => $user->{last_name},
		birth_date   => 0+$user->{birth_date},
	}));
}

sub get_visit {
	my ($res,$id,$q) = @_;
	my $visit = $VISITS[$id] or return $res->(404,'{}');
	return $res->(200, $JSON->encode({
		id          => 0+$visit->{id},
		location    => 0+$visit->{location},
		user        => 0+$visit->{user},
		visited_at  => 0+$visit->{visited_at},
		mark        => 0+$visit->{mark},
	}));
}

sub get_location {
	my ($res,$id,$q) = @_;
	my $loc = $LOCATIONS[$id] or return $res->(404,'{}');
	my $ret = {
		id       => 0+$loc->{id},
		country  => $COUNTRY_ID[$loc->{country}]{name},
		city     => $loc->{city},
		distance => 0+$loc->{distance},
		place    => $loc->{place},
	};
	return $res->(200, $JSON->encode($ret));
}

sub get_user_visits {
	my ($res,$id,$prm) = @_;
	my $user = $USERS[$id] or return $res->(404,'{}');

	# p $prm;
	my @cond;
	if (exists $prm->{fromDate}) {
		return $res->(400,'{}') unless $prm->{fromDate} =~ /^\d+$/;
		push @cond, "\$_->{visit}{visited_at} >= $prm->{fromDate}";
	}
	if (exists $prm->{toDate}) {
		return $res->(400,'{}') unless $prm->{toDate} =~ /^\d+$/;
		push @cond, "\$_->{visit}{visited_at} <= $prm->{toDate}";
	}
	if (exists $prm->{country}) {
		my $country = $COUNTRIES{ $prm->{country} };
		return $res->(400,'{"error":"Bad country"}') unless $country;
		push @cond, "\$_->{location}{country} == $country->{id}";
	}
	if (exists $prm->{toDistance}) {
		return $res->(400,'{}') unless $prm->{toDistance} =~ /^\d+$/;
		push @cond, "\$_->{location}{distance} < $prm->{toDistance}";
	}

	my $all = $USER_VISITS[ $id ]
		or return $res->(200, '{"visits":[]}');

	my @visits;
	my $filter;
	if (@cond) {
		my $body = 'sub{'.join(' and ', @cond).'}';
		$filter = eval $body or die $@;
	}
	for($filter ? grep &$filter, @$all : @$all ) {
		push @visits, {
			mark       => 0+$_->{visit}{mark},
			visited_at => 0+$_->{visit}{visited_at},
			place      => $_->{location}{place},
		};
	}
	return $res->(200, $JSON->encode({
		visits => \@visits,
	}));
}
sub get_location_avg {
	my ($res,$id,$prm) = @_;
	my $loc = $LOCATIONS[$id] or return $res->(404,'{}');

	my @cond;
	if (exists $prm->{fromDate}) {
		return $res->(400,'{}') unless $prm->{fromDate} =~ /^\d+$/;
		push @cond, "\$_->{visit}{visited_at} >= $prm->{fromDate}";
	}
	if (exists $prm->{toDate}) {
		return $res->(400,'{}') unless $prm->{toDate} =~ /^\d+$/;
		push @cond, "\$_->{visit}{visited_at} <= $prm->{toDate}";
	}

	if (exists $prm->{fromAge}) {
		return $res->(400,'{}') unless $prm->{fromAge} =~ /^\d+$/;
		my $older = $NOW->minus_years( $prm->{fromAge} )->epoch;
		push @cond, "\$_->{user}{birth_date} <= $older";
	}
	if (exists $prm->{toAge}) {
		return $res->(400,'{}') unless $prm->{toAge} =~ /^\d+$/;
		my $younger = $NOW->minus_years( $prm->{toAge} )->epoch;
		push @cond, "\$_->{user}{birth_date} > $younger";
	}
	if (exists $prm->{gender}) {
		return $res->(400,'{}') unless $prm->{gender} =~ /^(f|m)$/;
		push @cond, "\$_->{user}{gender} eq '$prm->{gender}'";
	}

	my $all = $LOCATION_VISITS[ $id ]
		or return $res->(200, '{"avg": 0}');

	my $sum = 0;
	my $cnt = 0;
	my $filter;
	if (@cond) {
		my $body = 'sub{'.join(' and ', @cond).'}';
		$filter = eval $body or die $@;
	}
	for ( $filter ? grep &$filter, @$all : @$all ) {
		$sum += $_->{visit}{mark};
		$cnt++;
	}
	return $res->(200, $JSON->encode({
		avg => $cnt && (int($sum/$cnt*1e5+0.5)/1e5) || 0,
	}))
}

sub update_user {
	my ($res,$id,$prm,$data) = @_;
	my $user = $USERS[$id] or return $res->(404,'{}');
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

	$user->{email}      = $data->{email}      if exists $data->{email};
	$user->{birth_date} = $data->{birth_date} if exists $data->{birth_date};
	$user->{gender}     = $data->{gender}     if exists $data->{gender};
	$user->{first_name} = $data->{first_name} if exists $data->{first_name};
	$user->{last_name}  = $data->{last_name}  if exists $data->{last_name};

	return;
}
sub update_location {
	my ($res,$id,$prm,$data) = @_;
	# if ($id == 123) {
	# 	warn "[[$id]]: ".$JSON->encode([$prm,$data,$LOCATIONS{$id}]);
	# }
	my $loc = $LOCATIONS[$id] or return $res->(404,'{}');

	length $data->{place}
		or return $res->(400,'{"error":"bad place"}')
		if exists $data->{place};

	length $data->{country} and length $data->{country} < 50
		or return $res->(400,'{"error":"bad country"}')
		if exists $data->{country};

	length $data->{city} and length $data->{city} < 50
		or return $res->(400,'{"error":"bad city"}')
		if exists $data->{city};

	$data->{distance} =~ /^\d+$/
		or return $res->(400,'{"error":"bad distance"}')
		if exists $data->{distance};

	$res->("200",'{}');

	$loc->{place} = $data->{place} if exists $data->{place};
	$loc->{distance} = $data->{distance} if exists $data->{distance};
	$loc->{city} = $data->{city} if exists $data->{city};
	$loc->{country} = get_country $data->{country} or die "No country" if exists $data->{country};

	return;
}

sub update_visit {
	my ($res,$id,$prm,$data) = @_;
	my $vis = $VISITS[$id] or return $res->(404,'{}');

	$data->{mark} =~ /^[0-5]$/
		or return $res->(400,'{"error":"bad mark"}')
		if exists $data->{mark};

	my $resort_loc;
	my $resort_usr;

	if (exists $data->{visited_at}) {
		$data->{visited_at} =~ /^\d+$/
			or return $res->(400,'{"error":"bad visited_at"}');
		$resort_loc = 1;
		$resort_usr = 1;
	}
	my $loc;
	my $user;


	if (exists $data->{location} and $vis->{location} != $data->{location}) {
		$loc = $LOCATIONS[$data->{location}] or return $res->(400,'{"error":"bad location"}');
		$resort_loc = 0;
	}
	if (exists $data->{user} and $vis->{user} != $data->{user}) {
		$user = $USERS[$data->{user}] or return $res->(400,'{"error":"bad user"}');
		$resort_usr = 0;
	}
	$res->(200,'{}');

	$vis->{mark} = $data->{mark} if exists $data->{mark};
	$vis->{visited_at} = $data->{visited_at} if exists $data->{visited_at};

	if ($loc) {
		my $cond;
		# if ($vis->{location} == 935 or $data->{location} == 935) {
		# 	p $vis;
		# 	p $data;
		# 	$cond = 1;
		# }
		my $oldv = $vis->{location};
		my $newv = $data->{location};

		my $old = $LOCATION_VISITS[ $vis->{location} ];
		my $new = $LOCATION_VISITS[ $data->{location} ] //= [];

		# dump_visits "Before", $old if DEBUG and $cond;
		$vis->{location} = $data->{location};
		my $ptr;

		# p $new if $cond;

		aefor 0, $#$old, sub {
			if ( $old->[$_]{visit}{id} == $vis->{id} ) {
				# warn "rem $_" if $cond;
				$ptr = splice @$old, $_,1, ();
				last;
			}
		}, sub {
			aefor 0, $#$new, sub {
				# warn "$_: $new->[$_]{visit}{visited_at} > $vis->{visited_at}" if $cond;
				last if $new->[$_]{visit}{visited_at} > $vis->{visited_at};
			}, sub {
				# warn "psh $_" if $cond;
				splice @$new,$_, 0, $ptr;
				$ptr->{location} = $loc;
			};
		};


		# for ( 0..$#$old ) {
		# 	if ( $old->[$_]{visit}{id} == $vis->{id} ) {
		# 		# warn "rem $_" if $cond;
		# 		$ptr = splice @$old, $_,1, ();
		# 		last;
		# 	}
		# }
		# my $pos;
		# for ($pos = 0; $pos < @$new; $pos++ ) {
		# 	last if $new->[$pos]{visit}{visited_at} > $vis->{visited_at};
		# }
		# # warn "psh $pos" if $cond;
		# splice @$new,$pos, 0, $ptr;
		# $ptr->{location} = $loc;
	}


	if ($user) {
		my $old = $USER_VISITS[ $vis->{user} ];
		my $new = $USER_VISITS[ $data->{user} ] //= [];
		$vis->{user} = $data->{user};
		my $ptr;

		aefor 0,$#$old, sub {
			if ( $old->[$_]{visit}{id} == $vis->{id} ) {
				$ptr = splice @$old, $_,1, ();
				last;
			}
		}, sub {
			aefor 0, $#$new, sub {
				last if $new->[$_]{visit}{visited_at} > $vis->{visited_at};
			},
			sub {
				splice @$new,$_, 0, $ptr;
				$ptr->{user} = $user;
			};
		};

		# for ( 0..$#$old ) {
		# 	if ( $old->[$_]{visit}{id} == $vis->{id} ) {
		# 		$ptr = splice @$old, $_,1, ();
		# 		last;
		# 	}
		# }
		# for ($pos = 0; $pos < @$new; $pos++ ) {
		# 	last if $new->[$pos]{visit}{visited_at} > $vis->{visited_at};
		# }
		# splice @$new,$pos, 0, $ptr;
		# $ptr->{user} = $user;
	}


	if ($resort_usr) {
		AE::postpone {
			my $visits = $USER_VISITS[ $vis->{user} ];
			@$visits = sort {
				$a->{visit}{visited_at} <=> $b->{visit}{visited_at}
			} @$visits;
		};
	}
	if ($resort_loc) {
		AE::postpone {
			my $visits = $LOCATION_VISITS[ $vis->{location} ];
			@$visits = sort {
				$a->{visit}{visited_at} <=> $b->{visit}{visited_at}
			} @$visits;
		};
	}
}

sub create_user {
	my ($res,undef,$prm,$data) = @_;
	($data->{id} !~ /^\d+$/ or $USERS[$data->{id}]) and return $res->(400,'{"error":"bad id"}');

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
	$USERS[ $data->{id} ] = $data;
	return;
}

sub create_location {
	my ($res,undef,$prm,$data) = @_;
	($data->{id} !~ /^\d+$/ or $LOCATIONS[$data->{id}]) and return $res->(400,'{"error":"bad id"}');

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
	$data->{country} = get_country $data->{country};
	$LOCATIONS[$data->{id}] = $data;
	return;
}

sub create_visit {
	my ($res,undef,$prm,$data) = @_;
	my $loc = $LOCATIONS[$data->{location}] or return $res->(400,'{"error":"bad location"}');
	my $usr = $USERS[$data->{user}] or return $res->(400,'{"error":"bad user"}');
	($data->{id} !~ /^\d+$/ or $VISITS[$data->{id}]) and return $res->(400,'{"error":"bad id"}');
	$data->{mark} =~ /^[0-5]$/ or return $res->(400,'{"error":"bad mark"}');
	$data->{visited_at} =~ /^\d+$/ or return $res->(400,'{"error":"bad visited_at"}');

	$res->(200,'{}');
	$VISITS[$data->{id}] = $data;

	AE::postpone {
		my $uvl = {
			user => $usr,
			visit => $data,
			location => $loc,
		};

		my $uv = $USER_VISITS[ $data->{user} ] //= [];
		my $lv = $LOCATION_VISITS[ $data->{location} ] //= [];

		my $pos;
		for ($pos = 0; $pos < @$lv; $pos++) {
			last if $lv->[$pos]{visit}{visited_at} > $data->{visited_at};
		}
		splice @$lv,$pos,0,$uvl;

		for ($pos = 0; $pos < @$uv; $pos++) {
			last if $uv->[$pos]{visit}{visited_at} > $data->{visited_at};
		}
		splice @$uv,$pos,0,$uvl;
	};

	return;
}

my $GET = Router::R3->new(
	'/users/{id:\d+}'         => \&get_user,
	'/visits/{id:\d+}'        => \&get_visit,
	'/locations/{id:\d+}'     => \&get_location,
	'/users/{id:\d+}/visits'  => \&get_user_visits,
	'/locations/{id:\d+}/avg' => \&get_location_avg,
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
	return $JSON->encode(\%STAT);
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

my $g;$g = EV::timer 0,10, sub {
	if ($prev == $cnt) {
		# warn "Same";
		if ($last_ac eq 'grow') {
			# warn "Stopped";
			warn sprintf "END[$CONNS:$REQS]; Cnt: %d; Min: %0.4fms; Max: %0.4fms; Avg: %0.4fms [%s]\n", $cnt, $min*1000, $max*1000, eval{$sum*1000/$cnt}, mystat();
			$prev = $cnt = $max = $sum = 0;
			$min = 1e10;
		}
		$last_ac = 'same';
	}
	else {
		# if ($last_ac eq 'same') {
		# 	warn "Start\n";
		# }
		warn sprintf "Grow[$CONNS:$REQS]: %+d, Cnt: %d; Min: %0.4fms; Max: %0.4fms; Avg: %0.4fms [%s]\n", $cnt-$prev, $cnt, $min*1000, $max*1000, eval{$sum*1000/$cnt}, mystat();
		$prev = $cnt;
		$last_ac = 'grow';
	}
};

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








