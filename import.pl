#!/usr/bin/env perl

use utf8;
use open qw(:utf8 :std);
use 5.016;
# use feature 'fc';
use JSON::XS;
use DDP;
use Time::HiRes qw(time);
use List::Util qw(max);
our $JSON = JSON::XS->new->utf8;

our %USERS;
# our %CITIES;
our %COUNTRIES; our $COUNTRY_MAX=0; # by name
our %COUNTRY_ID;
our %LOCATIONS;

our %VISITS;

our @VISIT_FIELDS = qw(location user visited_at mark);

our %USER_VISITS;
our %LOCATION_VISITS;

sub new_user {

}

sub new_location {

}

sub new_visit {
	my $v = shift;

}

my $start = time; my $count = 0;
for my $f (<users_*.json>) {
	my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{users}};
	printf "Loading %d users from %s\n",0+@$data,$f;
	for my $u (@$data) {
		$USERS{$u->{id}} = {
			_ => $u, # raw data for returning
		};
		++$count;
	}
}
printf "Loaded %d users in %0.4fs\n", $count, time-$start;

my $start = time; my $count = 0;
for my $f (<locations_*.json>) {
	my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{locations}};
	printf "Loading %d locations from %s\n",0+@$data,$f;
	for my $l (@$data) {
		# p $l unless %COUNTRIES;
		my $cnkey = fc($l->{country});
		my $country = $COUNTRIES{ $cnkey } ||= do {
			my $id = ++$COUNTRY_MAX;
			$COUNTRY_ID{$id} =
			+{ id => $id, key => $cnkey, name => $l->{country} }
		};
		$l->{country} = $country->{id};
		++$count;
		$LOCATIONS{$l->{id}} = $l;
	}
}
# p %COUNTRIES;
printf "Loaded %d locations in %0.4fs, found %d countries\n", $count, time-$start, 0+keys %COUNTRIES;

my $start = time; my $count = 0;
for my $f (<visits_*.json>) {
	my $data = do { open my $fl, '<:raw', $f or die "$!"; local $/; $JSON->decode(<$fl>)->{visits}};
	printf "Loading %d visits from %s\n",0+@$data,$f;
	for my $v (@$data) {
		# p $v;
		my $loc = $LOCATIONS{$v->{location}};
		# p $loc;
		my $usr = $USERS{$v->{user}};
		# p $usr;

		$VISITS{$v->{id}} = $v;
		push @{ $loc->{visits} }, {
			visited_at => $v->{visited_at},
			birth_date => $usr->{_}{birth_date},
			gender     => $usr->{_}{gender},
			_ => $v,
		};
		push @{ $usr->{visits} }, {
			visited_at => $v->{visited_at},
			country => $loc->{country},
			distance => $loc->{distance},
			_ => $v,
		};
		++$count;
	}
}
printf "Loaded %d visits in %0.4fs\n", $count, time-$start;

my $start = time;
my $max = 0;
for my $loc (values %LOCATIONS) {
	$loc->{visits} //= [];
	@{ $loc->{visits} } = sort {
		$a->{visited_at} <=> $b->{visited_at}
	} @{ $loc->{visits} };
	$max = max($max,0+@{ $loc->{visits} });
}
printf "Sorted %d visits in %0.4fs, locations/max: %s\n", $count, time-$start, $max;

my $start = time;
my $max = 0;
for my $loc (values %USERS) {
	$loc->{visits} //= [];
	@{ $loc->{visits} } = sort {
		$a->{visited_at} <=> $b->{visited_at}
	} @{ $loc->{visits} };
	$max = max($max,0+@{ $loc->{visits} });
}
printf "Sorted %d visits in %0.4fs, users/max: %s\n", $count, time-$start, $max;


use lib glob("libs/*/lib"),glob("libs/*/blib/lib"),glob("libs/*/blib/arch");

use AnyEvent::HTTP::Server;
use EV;
use Time::Moment;

my $srv = AnyEvent::HTTP::Server->new(
	host => 0,
	port => 8880,
	on_reply => sub {
		say "\t$_[1]{Status} $_[1]{ResponseTime}";
		# p @_;
	},
	cb => sub {
		my $req = shift;
		my $path = $req->path;
		printf "%s %s\n", $req->method, $req->uri;
		if ($req->method eq 'POST') {
			my $buf;
			return sub {
				unless (defined $buf) {
					$buf = $_[1];
				}
				else {
					$$buf .= ${$_[1]};
				}
				if ($_[0]) {
					my $data = eval { $JSON->decode( $$buf ) };
					$data and ref $data eq 'HASH'
						or do { warn $@; return $req->reply(400, '{}'); };

					p $data;
					if ($path =~ m{^/users/(?:(\d+)|(new))$}) {
						if ($1) { # update

						}
						else {

						}
					}
					elsif($path =~ m{^/locations/(?:(\d+)|(new))$}) {

					}
					elsif($path =~ m{^/visits/(?:(\d+)|(new))$}) {
						my $v = $data;

						if ($1) { # update
							my $vis = $VISITS{$1}
								or return $req->reply(404,'{}');
							p $vis;

							if (exists $v->{mark}) {

							}

							if (exists $v->{location} and $vis->{location} != $v->{location}) {
								my $loc = $LOCATIONS{$v->{location}}
									or return $req->reply(404,'{"error":"bad location"}');
								# change loc
							}
							if (exists $v->{user} and $vis->{user} != $v->{user}) {
								my $loc = $LOCATIONS{$v->{user}}
									or return $req->reply(404,'{"error":"bad user"}');
								# change user
							}

							for (qw(visited_at mark)) {
								if (exists $v->{$_}) {
									$vis->{$_}{}
									if ($v->{$_} == $vis->{$_}) {
										delete $v->{$_};
									}
									else {
										say "= $vis->{$_}  => $v->{$_}";
									}
								}
							}
							return $req->reply(200,'{}') unless %$v;
							return $req->reply(501,'{}');

						}
						else {
							my $loc = $LOCATIONS{$v->{location}}
								or return $req->reply(400,'{"error":"bad location"}');
							my $usr = $USERS{$v->{user}}
								or return $req->reply(400,'{"error":"bad user"}');

							$VISITS{$v->{id}}
								and return $req->reply(400,'{}');

							warn "new";

							$VISITS{$v->{id}} = $v;
							my $pos;
							for $pos (0..$#{ $loc->{visits} }) {
								last if $v->{visited_at} > $_->{visited_at};
							}
							splice @{$loc->{visits}},$pos,0,{
								visited_at => $v->{visited_at},
								birth_date => $usr->{_}{birth_date},
								gender     => $usr->{_}{gender},
								_ => $v,
							};

							for $pos (0..$#{ $usr->{visits} }) {
								last if $v->{visited_at} > $_->{visited_at};
							}

							splice @{$usr->{visits}},$pos,0, {
								visited_at => $v->{visited_at},
								country => $loc->{country},
								distance => $loc->{distance},
								_ => $v,
							};
						}
					}
					else {
						return $req->reply(404,'{}');
					}
					$req->reply(200,'{}');
				}
			};
		}
		if ($path =~ m{^/users/(\d+)(/visits|)$}) {
			my $user = $USERS{$1}
				or return 404,'{}';
			unless ($2) {
				return 200, $JSON->encode($user->{_});
			}
			else {
				my $prm = $req->params;
				my @cond;
				if (exists $prm->{fromDate}) {
					return 400,'{}' unless $prm->{fromDate} =~ /^\d+$/;
					push @cond, "\$_->{visited_at} >= $prm->{fromDate}";
				}
				if (exists $prm->{toDate}) {
					return 400,'{}' unless $prm->{toDate} =~ /^\d+$/;
					push @cond, "\$_->{visited_at} <= $prm->{toDate}";
				}
				if (exists $prm->{country}) {
					my $country = $COUNTRIES{ fc $prm->{country} };
					return 400,'{"error":"Bad country"}' unless $country;
					push @cond, "\$_->{country} == $country->{id}";
				}
				if (exists $prm->{toDistance}) {
					my $country = $COUNTRIES{ $prm->{country} };
					return 400,'{}' unless $prm->{toDistance} =~ /^\d+$/;
					push @cond, "\$_->{distance} < $prm->{toDistance}";
				}

				my @visits;

				if (@cond) {
					my $body = 'sub{'.join(' and ', @cond).'}';
					say $body;
					my $sub = eval $body or die $@;
					for(grep &$sub, @{ $user->{visits} } ) {
						push @visits, {
							mark => $_->{_}{mark},
							visited_at => $_->{_}{visited_at},
							place => $LOCATIONS{$_->{_}{location}}{place},
						};
					}
				}
				else {
					# p $user->{visits};
					for ( @{ $user->{visits} } ) {
						push @visits, {
							mark => $_->{_}{mark},
							visited_at => $_->{_}{visited_at},
							place => $LOCATIONS{$_->{_}{location}}{place},
						};
					}
				}
				return 200, $JSON->encode({
					visits => \@visits,
				});
			}
		}
		elsif ($path =~ m{^/locations/(\d+)(/avg|)$}) {
			my $loc = $LOCATIONS{$1}
				or return 404,'{}';
			say "Location $1 + ".(0+@{ $loc->{visits} });

			unless ($2) {
				# p $loc;
				# p $COUNTRIES{$loc->{country}};
				my $ret = {
					id => $loc->{id},
					country => $COUNTRY_ID{$loc->{country}}{name},
					city    => $loc->{city},
					distance => $loc->{distance},
					place    => $loc->{place},
				};
				# p $ret;
				return 200, $JSON->encode($ret);
			}
			else {
				my $prm = $req->params;

				my $now = Time::Moment->now;
				my @cond;
				if (exists $prm->{fromDate}) {
					return 400,'{}' unless $prm->{fromDate} =~ /^\d+$/;
					push @cond, "\$_->{visited_at} >= $prm->{fromDate}";
				}
				if (exists $prm->{toDate}) {
					return 400,'{}' unless $prm->{toDate} =~ /^\d+$/;
					push @cond, "\$_->{visited_at} <= $prm->{toDate}";
				}

				if (exists $prm->{fromAge}) {
					return 400,'{}' unless $prm->{fromAge} =~ /^\d+$/;
					my $older = $now->minus_years( $prm->{fromAge} )->epoch;
					push @cond, "\$_->{birth_date} <= $older";
				}
				if (exists $prm->{toAge}) {
					return 400,'{}' unless $prm->{toAge} =~ /^\d+$/;
					my $younger = $now->minus_years( $prm->{toAge} )->epoch;
					push @cond, "\$_->{birth_date} > $younger";
				}
				if (exists $prm->{gender}) {
					return 400,'{}' unless $prm->{gender} =~ /^(f|m)$/;
					push @cond, "\$_->{gender} eq '$prm->{gender}'";
				}
				my $sum = 0;
				my $cnt = 0;
				if (@cond) {
					my $body = 'sub{'.join(' and ', @cond).'}';
					# say $body;
					my $sub = eval $body or die $@;
					for ( grep &$sub, @{ $loc->{visits} } ) {
						# p $_;
						$sum += $_->{_}{mark};
						$cnt++;
					}
				}
				else {
					for ( @{ $loc->{visits} } ) {
						$sum += $_->{_}{mark};
						$cnt++;
					}
				}
				return 200, $JSON->encode({
					avg => $cnt && (int($sum/$cnt*1e5+0.5)/1e5) || 0,
				})
			}
		}
		elsif ($path =~ m{^/visits/(\d+)$}) {
			my $visit = $VISITS{$1}
				or return 404,'{}';
			return 200, $JSON->encode($visit);
		}
		else {
			return 404,'{}', headers => { connection => 'close' };
		}
	}
);
say $srv->listen;
say $srv->accept;
EV::loop;


__END__
User:
	id: int32,
	email: str[100], uniq
	gender: f|m
	first_name: str[50]
	last_name: str[50]
	birth_date: unix timestamp
index(id, hash)

Location:
	id: int32,
	place: text,
	country: str[50],
	city: str[50],
	distance: int32,

Visit:
	id: int32,
	location: fk(location)
		+country -> location.country
	user: fk(user)
		+bd -> user.bd
	visited_at: timestamp
	mark: int, [0..5]


+Countries
	id: int32
	name: str[50]

+Cities:
	id: int32
	country: fk(countries)
	name: str[50]

Queries:
1. GET /users/$id
	Index: HASH: users#id -> User
	
	-> User

2. GET /users/<id>/visits
	fromDate - посещения с visited_at > fromDate
	toDate - посещения с visited_at < toDate
	country - название страны, в которой находятся интересующие достопримечательности
	toDistance - возвращать только те места, у которых расстояние от города меньше этого параметра

Index1: visit#visited_at, filter: country, distance
Index2: country + visit#visited_at, filter: distance

	-> Visits

3. GET /locations/<id>/avg
	fromDate - учитывать оценки только с visited_at > fromDate
	toDate - учитывать оценки только с visited_at < toDate
	fromAge - учитывать только путешественников, у которых возраст (считается от текущего timestamp) больше этого параметра
	toAge - как предыдущее, но наоборот
	gender - учитывать оценки только мужчин или женщин

Index1: visit#visited_at, filter: bd, gender
Index2: gender, visit#visited_at, filter: bd
