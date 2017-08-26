#!/usr/bin/env perl

use 5.016;
use lib qw(blib/lib blib/arch);
use Benchmark qw(:all);
use JSON::XS;
our $JSON = JSON::XS->new->utf8;
use Local::HLCup;
use Test::More;
use DDP;

our %USERS;
$USERS{123} = {
	id           => 123,
	email        => 'test@test',
	gender       => 'm',
	first_name   => 'Mons',
	last_name    => 'Anderson',
	birth_date   => 12345678,
};
our %LOCATIONS;
$LOCATIONS{345} = {
	id => 345,
	country => "test", # $db->get_country("test")
	distance => 45,
	city => "big city",
	place => "some place",
};
our %VISITS;
$VISITS{1} = {
	id => 1,
	user => 123,
	location => 345,
	mark => 3,
	visited_at => time(),
};
$VISITS{2} = {
	id => 2,
	user => 123,
	location => 345,
	mark => 2,
	visited_at => time()-10,
};
my $db = Local::HLCup->new();



$db->add_user(@{ $USERS{123} }{qw( id email first_name last_name gender birth_date)});
# p $db->get_user(123);
is_deeply
	$JSON->decode($db->get_user(123)), $USERS{123},
	'existing user'
	or p $db->get_user(123)
	for 1..2;
# say $db->get_user(123);
# say $db->get_user(124);

my $id = $db->get_country("test");
ok $id, 'have country id';
is $id, $db->get_country("test"), 'contry id the same';
isnt $id, $db->get_country("other"), 'new country - new id';
is $db->get_country("other"),$db->get_country("other"), 'other id eq';


my $loc = $LOCATIONS{345};
$db->add_location($loc->{id},$db->get_country($loc->{country}), @{$loc}{ qw(distance city place) });
diag $db->get_location(345);
is_deeply
	$JSON->decode($db->get_location($loc->{id})), $LOCATIONS{$loc->{id}},
	'existing location'
	or p $db->get_location($loc->{id})
	for 1..2;

my $vis1 = $VISITS{1};
$db->add_visit(@{$vis1}{qw(id user location mark visited_at)});

my $vis2 = $VISITS{2};
$db->add_visit(@{$vis2}{qw(id user location mark visited_at)});

# $db->add_visit(-1, 124, 344, 3, time());
# $db->add_visit(2, 123, 345, 3, time()-10);
p $db->get_visit($vis1->{id});
is_deeply
	$JSON->decode($db->get_visit($vis1->{id})), $VISITS{$vis1->{id}},
	'existing visit'
	or p $db->get_visit($vis1->{id})
	for 1..2;

say $db->get_location_avg(345,0,2**31-1,-2**31+1,2**31-1);
say $db->get_location_avg(345,0,2**31-1,0,0);

say $db->get_user_visits(123,0,2**31-1,0,0);
ok $JSON->decode($db->get_user_visits(123,0,2**31-1,0,0));
ok $JSON->decode($db->get_user_visits(123,0,0,0,0));

__END__
cmpthese timethese 1e6, {
	xs => sub { my $re = $db->get_user(123); },
	pp => sub {
		my $user = $USERS{123};
		my $re = $JSON->encode({
			id           => 0+$user->{id},
			email        => $user->{email},
			gender       => $user->{gender},
			first_name   => $user->{first_name},
			last_name    => $user->{last_name},
			birth_date   => 0+$user->{birth_date},
		});

	},
}