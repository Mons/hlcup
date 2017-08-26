#define PERL_NO_GET_CONTEXT

#ifdef __cplusplus
extern "C" {
#endif
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"
// #include "rb_tree.h"
#ifdef __cplusplus
}
#endif

#include <map>
#include <string>

typedef struct User User;
typedef struct Visit Visit;
typedef struct Location Location;

typedef struct VisitSet {
	Visit    * visit;
	User     * user;
	Location * location;
} VisitSet;

typedef struct User {
	int  id;
	int  birth_date;
	char gender;
	SV  *email;
	SV  *first_name;
	SV  *last_name;

	std::map<int,VisitSet *> visits; // visited_at + VS
} User;

// typedef std::map<int, User> mapUser;

typedef struct Country {
	int id;
	SV * name;
} Country;
// typedef std::map<int, Country> mapCountry;

typedef struct Location {
	int id;
	int country;
	int distance;
	SV *place;
	SV *city;

	std::multimap<int,VisitSet *> visits; // visited_at + VS
} Location;
// typedef std::map<int, Location> mapLocation;

typedef struct Visit {
	int id;
	int location;
	int user;
	int visited_at;
	int mark;
} Visit;

// typedef std::map<int, Location> mapVisit;



int vs_cmp (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

/*
	User(id)->
		list of visits
*/
// sorted by visited_at
// visited_at is not unique

// typedef std::map<int, std::list<VisitSet> > mapUserVisits;
// typedef std::map<int, std::list<VisitSet> > mapLocationVisits;


typedef struct HLCup {
	std::map<int,User *> *users;
	std::map<int,Location *> *locations;
	std::map<int,std::string> *country_id;
	std::map<std::string,int> *countries;

	std::map<int,Visit *> *visits;
	int country_max;
} HLCup;

// class HLCup {
// 	public:
// 		std::map<int,User *> users;
// 		std::map<int,Location *> locations;
// 		std::map<int,Visit *> visits;
// 		std::map<int,std::string> country_id;
// 		std::map<std::string,int> countries;
// 		int country_max;

// };

int intcmp (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}


// class HLCup {
// 	private:
// 		// std::auto_ptr< std::map<int,User> > users;
// 		mapUser users;
// 	public:
// 		HLCup () {
// 			warn("create obj");
// 		};
// 		~HLCup () {
// 			warn("destroy obj");
// 		};
// 		void add_user (int id, SV * email, SV * first_name, SV * last_name, char gender, int birth_date) {
// 			warn("add_user %d %c %d (%s %s %s)",id, gender, birth_date, SvPVX(email), SvPVX(first_name), SvPVX(last_name));
// 			SvREFCNT_inc(email);
// 			User u = {.id = id, .birth_date=birth_date, .gender=gender, .email = email};
// 			users[id] = u;
// 		};

// 		SV * get_user (aTHX_ int id) {
// 			mapUser::iterator it = users.find(id);
// 			if( it != users.end() ) {
// 				User * u = &it->second;
// 				warn("got it %s", SvPVX(it->second.email));
// 				warn("got it %s", SvPVX(u->email));
// 				// std::cout << "B: " << it->second << "\n";
// 				HV *rv = newHV();
// 				SV *ret = newRV_noinc(rv);
// 				// hv_stores(rv,"id",newSViv(u->id));
// 			}
// 			else {
// 				warn("no");
// 				return NULL;
// 			}
// 		}
// };
// void
// HLCup::add_user(int id, SV * email, SV * first_name, SV * last_name, char gender, int birth_date)

// void
// HLCup::get_user(aTHX_ int id)

#define dSVXF(sv,ref,type) \
	SV *sv = newSV( sizeof(type) );\
	SvUPGRADE( sv, SVt_PV ); \
	SvCUR_set(sv,sizeof(type)); \
	SvPOKp_on(sv); \
	type * ref = (type *) SvPVX( sv ); \
	memset(ref,0,sizeof(type)); \

#define dSVX(sv,ref,type) \
	SV *sv = newSV( sizeof(type) );\
	SvUPGRADE( sv, SVt_PV ); \
	type * ref = (type *) SvPVX( sv ); \

#define mycpy(dst,src) do { memcpy(dst,src,sizeof(src)-1);dst+=sizeof(src)-1; } while(0)


MODULE = Local::HLCup		PACKAGE = Local::HLCup

void new(SV *)
	PPCODE:
		HLCup * self = (HLCup *) safemalloc( sizeof(HLCup) );
		// HLCup self = HLCup();

		// HLCup * self = (HLCup *) safemalloc( sizeof(HLCup) );
		// memset(self,0,sizeof(HLCup));
		SV *iv = newSViv(PTR2IV( self ));
		ST(0) = sv_2mortal(sv_bless(newRV_noinc(iv), gv_stashpv(SvPV_nolen(ST(0)), TRUE)));
		self->users = new std::map<int,User*>;
		self->country_max = 0;
		self->countries = new std::map<std::string,int>;
		self->country_id = new std::map<int,std::string>;
		self->locations = new std::map<int,Location*>;
		self->visits = new std::map<int,Visit*>;

		// self->Countries = newHV();
		// self->CountryID = newHV();
		// self->Locations = newHV();
		// self->Visits = newHV();
		// self->UserVisits = newHV();
		// self->LocationVisits = newHV();

		XSRETURN(1);

void DESTROY(SV *)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		// if (self->UserVisits) free(self->UserVisits);
		// xs_ev_cnn_self(ScCnn);
		
		// if (!PL_dirty && self->reqs) {
		// 	//TODO
		// 	free_reqs(self, "Destroyed");
		// 	SvREFCNT_dec(self->reqs);
		// 	self->reqs = 0;
		// }
		// xs_ev_cnn_destroy(self);

void add_user(SV *, int id, SV * email, SV * first_name, SV * last_name, char gender, int birth_date)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		// warn("add_user %d %c %d (%s %s %s)",id, gender, birth_date, SvPVX(email), SvPVX(first_name), SvPVX(last_name));
			// int id;
			// int birth_date;
			// char gender;
			// SV *email;
			// SV *first_name;
			// SV *last_name;
		User * user = new User;
		// if (id == 404) {
		// 	warn("ins %d <%s>",id,SvPVX(email));
		// }
		user->id = id;
		user->birth_date = birth_date;
		user->gender = gender;
		user->email = SvREFCNT_inc(email);
		user->first_name = SvREFCNT_inc(first_name);
		user->last_name = SvREFCNT_inc(last_name);
		(*self->users)[id] = user;

		XSRETURN_UNDEF;

void get_user(SV *, int id)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		// warn("Lookup %d",id);
		std::map<int,User*>::iterator it = (*self->users).find(id);
		if ( it != (*self->users).end() ) {
			User * user = (*it).second;
			// warn("Have key: %d/%d <%s>", (*it).first, user->id, SvPVX(user->email));
			SV *rv = newSV(100 + (SvCUR( user->email ) + SvCUR(user->first_name) + SvCUR(user->last_name)) * 2);
			SvUPGRADE( rv, SVt_PV );
			SvPOKp_on(rv);
			char *p = SvPVX(rv);

			// {"id":,"birth_date":,"gender":"","email":"","first_name":"","last_name":""}

			mycpy(p,"{\"id\":");
			p += snprintf(p, 14, "%d", user->id);
			mycpy(p,",\"birth_date\":");
			p += snprintf(p, 14, "%d", user->birth_date);
			mycpy(p,",\"gender\":\"");
			*p++ = user->gender;
			mycpy(p,"\",\"email\":\"");

			memcpy( p, SvPVX(user->email),SvCUR(user->email) );
			p+=SvCUR(user->email);
			
			mycpy(p,"\",\"first_name\":\"");

			memcpy( p, SvPVX(user->first_name),SvCUR(user->first_name) );
			p+=SvCUR(user->first_name);

			mycpy(p,"\",\"last_name\":\"");

			memcpy( p, SvPVX(user->last_name),SvCUR(user->last_name) );
			p+=SvCUR(user->last_name);
			
			mycpy(p,"\"}\n");

			SvCUR_set(rv,p - SvPVX(rv));
			ST(0) = rv;
			XSRETURN(1);
		} else {
			warn("No user for %d",id);
			XSRETURN_UNDEF;
		}

void get_country(SV *, SV * country)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		std::string cn( SvPVX(country) );
		std::map<std::string,int>::iterator it = (*self->countries).find(cn);
		if ( it != (*self->countries).end() ) {
			ST(0) = sv_2mortal(newSViv( (*it).second ));
		}
		else {
			int id = ++self->country_max;
			(*self->countries)[cn] = id;
			(*self->country_id)[id] = cn;
			ST(0) = sv_2mortal(newSViv( id ));
		}
		XSRETURN(1);

void add_location(SV *, int id, int country, int distance, SV *city, SV *place )
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		Location * loc = new Location;
		loc->id = id;
		loc->country = country;
		loc->distance = distance;
		loc->city = SvREFCNT_inc(city);
		loc->place = SvREFCNT_inc(place);
		(*self->locations)[id] = loc;

		XSRETURN_UNDEF;

void get_location(SV *, int id)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		std::map<int, Location*>::iterator it = (*self->locations).find(id);
		if ( it != (*self->locations).end() ) {
			// warn("Have key: %d", id);
			Location * loc = (*it).second;
			SV *rv = newSV(200 + (SvCUR( loc->city ) + SvCUR(loc->place)) * 2);
			SvUPGRADE( rv, SVt_PV );
			SvPOKp_on(rv);
			char *p = SvPVX(rv);

			// {"id":,"distance":,"country":"","city":"","place":""}

			mycpy(p,"{\"id\":");
			p += snprintf(p, 14, "%d", loc->id);
			mycpy(p,",\"distance\":");
			p += snprintf(p, 14, "%d", loc->distance);

			mycpy(p,",\"city\":\"");

			memcpy( p, SvPVX(loc->city),SvCUR(loc->city) );
			p+=SvCUR(loc->city);
;
			std::map<int,std::string>::iterator cnit = (*self->country_id).find(loc->country);

			if( cnit != (*self->country_id).end()) {
				mycpy(p,"\",\"country\":\"");
				memcpy( p, (*cnit).second.c_str(), (*cnit).second.length());
				p+= (*cnit).second.length();

			}
			
			mycpy(p,"\",\"place\":\"");

			memcpy( p, SvPVX(loc->place),SvCUR(loc->place) );
			p+=SvCUR(loc->place);
			
			mycpy(p,"\"}\n");

			SvCUR_set(rv,p - SvPVX(rv));
			ST(0) = rv;
			XSRETURN(1);

		}
		else {
			XSRETURN_UNDEF;
		}



void add_visit(SV *, int id, int user, int location, int mark, int visited_at )
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		Visit * vis = new Visit;
		vis->id = id;
		vis->location = location;
		vis->user = user;
		vis->mark = mark;
		vis->visited_at = visited_at;
		Location *loc = (*self->locations)[location];
		User *usr = (*self->users)[user];
		// warn("loc=%p, user=%p",loc,usr);
		if (loc && usr) {
			VisitSet *vs = new VisitSet;
			vs->visit = vis;
			vs->user = usr;
			vs->location = loc;
			// warn("%d -> %p",vis->visited_at, vs);

			// usr->visits[vis->visited_at] = vs;
			(*self->visits)[id] = vis;
			usr->visits.insert(std::pair< int,VisitSet* >(vis->visited_at, vs));
			loc->visits.insert(std::pair< int,VisitSet* >(vis->visited_at, vs));
		}
		else{
			croak("XXX: No user or location");
		}
		/*
		SV **key;
		std::map<int,VisitSet*> *vs;
		key = hv_fetch(self->UserVisits,(char *)&user,sizeof(int),0);
		if (key) {
			vs = (std::map<int,VisitSet*> *) SvIV( *key );
			warn("Found: %p", vs);
		}
		else {
			vs = new std::map<int,VisitSet*>;
			warn("Created: %p", vs);
			(void)hv_store(self->UserVisits, (char *)&user,sizeof(int), newSViv(PTR2IV( vs )), 0);
		}

		VisitSet * cur = (VisitSet *) safemalloc(sizeof(VisitSet));

		cur->visit = vis;

		for (std::multimap<int, VisitSet*>::iterator it = (*vs).begin(); it != (*vs).end(); ++it) {
			warn("item: %d; -> vis:%p: %d",(*it).first, (*it).second->visit, (*it).second->visit->id);
		}

		vs->insert(std::pair< int,VisitSet* >(vis->visited_at, cur));

		for (std::multimap<int, VisitSet*>::iterator it = (*vs).begin(); it != (*vs).end(); ++it) {
			warn("item: %d; -> vis:%p: %d",(*it).first, (*it).second->visit, (*it).second->visit->id);
		}
		*/
		XSRETURN_UNDEF;

void get_visit(SV *, int id)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		std::map<int, Visit*>::iterator it = (*self->visits).find(id);
		if ( it != (*self->visits).end() ) {
			// warn("Have key: %d", id);
			Visit * vis = (*it).second;
			SV *rv = newSV(100);
			SvUPGRADE( rv, SVt_PV );
			SvPOKp_on(rv);
			char *p = SvPVX(rv);

			// {"id":,"user":,"location":,"mark":,"visited_at":}

			mycpy(p,"{\"id\":");
			p += snprintf(p, 14, "%d", vis->id);
			mycpy(p,",\"user\":");
			p += snprintf(p, 14, "%d", vis->user);
			mycpy(p,",\"location\":");
			p += snprintf(p, 14, "%d", vis->location);
			mycpy(p,",\"mark\":");
			p += snprintf(p, 14, "%d", vis->mark);
			mycpy(p,",\"visited_at\":");
			p += snprintf(p, 14, "%d", vis->visited_at);
			mycpy(p,"}\n");

			SvCUR_set(rv,p - SvPVX(rv));
			ST(0) = rv;
			XSRETURN(1);

		}
		else {
			// warn("Not found: %d", id);
			XSRETURN_UNDEF;
		}

void get_location_avg(SV *, int id, int from, int till, int from_age, int till_age, char gender)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		Location *loc = (*self->locations)[id]; // TODO: find
		if (!loc) XSRETURN_UNDEF;

		std::multimap<int, VisitSet*>::iterator it;
		// warn("lookup from %d for %d",from, gender);
		if (from) {
			it = loc->visits.lower_bound(from);
		}
		else {
			it = loc->visits.begin();
		}

		warn("%d < visited_at < %d; %d < age < %d (%c)", from, till, from_age, till_age, gender);
		int sum = 0;
		int cnt = 0;

		for (; it != loc->visits.end(); ++it) {
			// warn("XXX %d ~~ %d (%d)",(*it).second->visit->visited_at, till, (*it).second->user->birth_date);
			if ( (*it).second->visit->visited_at > till ) break;
			if ( (*it).second->user->birth_date <= from_age ) continue;
			if ( (*it).second->user->birth_date > till_age ) continue;
			if ( gender && (*it).second->user->gender != gender ) continue;
			sum += (*it).second->visit->mark;
			cnt++;
			// warn("item: %d; -> user bd: %d",(*it).first, (*it).second->user->birth_date);
		}

		SV *rv = newSV(22);
		SvUPGRADE( rv, SVt_PV );
		SvPOKp_on(rv);
		char *p = SvPVX(rv);

		// {"avg":,}

		if (cnt > 0) {
			p += snprintf(p, 22, "{\"avg\":%.6g}\n", (double)sum/cnt);
		} else {
			mycpy(p,"{\"avg\":0}\n");
		}
		SvCUR_set(rv,p - SvPVX(rv));
		ST(0) = rv;
		XSRETURN(1);

void get_user_visits(SV *, int id, int from, int till, int country, int distance)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		std::map<int,User*>::iterator find = (*self->users).find(id);
		User *usr;
		if ( find != (*self->users).end() ) {
			usr = (*find).second;
		}
		else {
			XSRETURN_UNDEF;
			return;
		}

		// User *usr = (*self->users)[id]; // TODO: find
		// if (!usr) XSRETURN_UNDEF;

		std::multimap<int, VisitSet*>::iterator it;
		if (from) {
			it = usr->visits.lower_bound(from);
		}
		else {
			it = usr->visits.begin();
		}

		warn("user_visits: %d < visited_at < %d; country: %d, distance: %d for <%s>", from, till, country, distance, SvPVX(usr->email));

		SV *rv = sv_2mortal(newSV(1024));
		// sv_dump((SV*)0x7fecd9a21c08);
		warn("rv = %p", rv);
		SvUPGRADE( rv, SVt_PV );
		warn("upgrade ok");
		SvPOKp_on(rv);
		char *p = SvPVX(rv);
		warn("pv = %p + %d",p, SvLEN(rv));
		mycpy(p,"{\"visits\":[\n");
		warn("pv = %p",p);
		bool first = true;

		warn("call loop %p",&usr->visits);
		for (; it != usr->visits.end(); ++it) {
			warn("XXX %d ~~ %d (%d)",(*it).second->visit->visited_at, till, (*it).second->user->birth_date);
			if ( (*it).second->visit->visited_at > till ) break;

			if ( country && (*it).second->location->country != country ) continue;
			if ( distance && (*it).second->location->distance > distance ) continue;
			Visit *vis = (*it).second->visit;
			Location *loc = (*it).second->location;

			// warn("item: %d; -> loc: %d",(*it).first, (*it).second->visit->location);
			if (!first) {
				mycpy(p,",\n\t{\"mark\":");
			}
			else {
				mycpy(p,"\t{\"mark\":");
				first = false;
			}
			p += snprintf(p, 14, "%d", vis->mark);
			mycpy(p,",\"visited_at\":");
			p += snprintf(p, 14, "%d", vis->visited_at);
			mycpy(p,",\"place\":\"");
			memcpy( p, SvPVX(loc->place),SvCUR(loc->place) );
			p+=SvCUR(loc->place);
			mycpy(p,"\"}");

		}
		mycpy(p,"\n]}\n");
		SvCUR_set(rv,p - SvPVX(rv));
		ST(0) = rv;
		XSRETURN(1);


