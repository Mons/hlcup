#define PERL_NO_GET_CONTEXT

#ifdef __cplusplus
extern "C" {
#endif
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "ppport.h"
#include <sys/mman.h>	// Needed for mlockall()

#ifdef __cplusplus
}
#endif

#include <map>
#include <string>
#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)


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

typedef struct Country {
	int id;
	SV * name;
} Country;

typedef struct Location {
	int id;
	int country;
	int distance;
	SV *place;
	SV *city;

	std::multimap<int,VisitSet *> visits; // visited_at + VS
} Location;

typedef struct Visit {
	int id;
	int location;
	int user;
	int visited_at;
	int mark;
} Visit;

typedef struct HLCup {
	std::map<int,User *> *users;
	std::map<int,Location *> *locations;
	std::map<int,std::string> *country_id;
	std::map<std::string,int> *countries;

	std::map<int,Visit *> *visits;
	int country_max;
} HLCup;

#define mycpy(dst,src) do { memcpy(dst,src,sizeof(src)-1);dst+=sizeof(src)-1; } while(0)

#define location_lookup(sym,id) \
Location *sym; \
std::map<int,Location*>::iterator location_it = (*self->locations).find(id) \
if ( location_it != (*self->locations).end() ) { \
	sym = (*location_it).second; \
} \
else { \
	croak("Failed to lookup location %d",(id)); \
}

SV * RV_200;
SV * RV_400;
SV * RV_404;
SV * RV_EMPTY;
HLCup * DEFAULT;

#define RETURN_400 ST(0) = RV_400; ST(1) = RV_EMPTY; XSRETURN(2);
#define RETURN_404 ST(0) = RV_404; ST(1) = RV_EMPTY; XSRETURN(2);
// #define RETURN_ST(sv) STMT_START { ST(0) = RV_200; ST(1) = sv_2mortal(sv); XSRETURN(2); } STMT_END
#define RETURN_ST(sv) STMT_START { ST(0) = RV_200; ST(1) = sv; XSRETURN(2); } STMT_END

int get_country(HLCup *self, SV * country) {
	std::string cn( SvPVX(country) );
	std::map<std::string,int>::iterator it = (*self->countries).find(cn);
	if ( it != (*self->countries).end() ) {
		return (*it).second;
	}
	else {
		int id = ++self->country_max;
		(*self->countries)[cn] = id;
		(*self->country_id)[id] = cn;
		return id;
	}
}

MODULE = Local::HLCup		PACKAGE = Local::HLCup
PROTOTYPES: disable
BOOT:
	RV_200 = newSViv(200);
	RV_400 = newSViv(400);
	RV_404 = newSViv(404);
	RV_EMPTY = newSVpvs("{}");

void mlockall()
	PPCODE:
		if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0)
			warn("mlockall failed: %s", strerror(errno));

void new(SV *)
	PPCODE:
   		HLCup * self = (HLCup *) safemalloc( sizeof(HLCup) );
   		DEFAULT = self;
		ST(0) = sv_2mortal(sv_bless(newRV_noinc(newSViv(PTR2IV( self ))), gv_stashpv(SvPV_nolen(ST(0)), TRUE)));

		self->users = new std::map<int,User*>;
		self->country_max = 0;
		self->countries = new std::map<std::string,int>;
		self->country_id = new std::map<int,std::string>;
		self->locations = new std::map<int,Location*>;
		self->visits = new std::map<int,Visit*>;

		XSRETURN(1);

void DESTROY(SV *)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );

		// if (!PL_dirty) {
		// 	//TODO
		// }

void add_user(SV *, int id, SV * email, SV * first_name, SV * last_name, char gender, int birth_date)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		User * user = new User;
		user->id = id;
		user->birth_date = birth_date;
		user->gender = gender;
		user->email = SvREFCNT_inc(email);
		user->first_name = SvREFCNT_inc(first_name);
		user->last_name = SvREFCNT_inc(last_name);
		(*self->users)[id] = user;

		XSRETURN_UNDEF;

void update_user(SV *, int id, SV * email, SV * first_name, SV * last_name, char gender, SV * birth_date)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		User * user;
		std::map<int, User*>::iterator it = (*self->users).find(id);
		if ( it != (*self->users).end() ) {
			user = (*it).second;
		} else {
			warn("No user for update");
			XSRETURN_UNDEF;
		}
		if (SvOK(birth_date)) {
			user->birth_date = SvIV(birth_date);
		}
		if (gender)
			user->gender = gender;
		if (SvOK(email)) {
			SvREFCNT_dec(user->email);
			user->email = SvREFCNT_inc(email);
		}
		if (SvOK(first_name)) {
			SvREFCNT_dec(user->first_name);
			user->first_name = SvREFCNT_inc(first_name);
		}
		if (SvOK(last_name)) {
			SvREFCNT_dec(user->last_name);
			user->last_name = SvREFCNT_inc(last_name);
		}

		XSRETURN_UNDEF;

void exists_user(SV *, SV * id)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		if (SvOK(id)) {
			std::map<int, User*>::iterator it = (*self->users).find(SvIV(id));
			if ( it != (*self->users).end() ) {
				ST(0) = &PL_sv_yes;
				XSRETURN(1);
			}
		}
		XSRETURN_UNDEF;

void get_user_rv(int id, SV*, SV*)
	PPCODE:
		register HLCup *self = DEFAULT;
		std::map<int,User*>::iterator it = (*self->users).find(id);
		if ( it != (*self->users).end() ) {
			User * user = (*it).second;
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
			*p = 0;
			SvCUR_set(rv,p - SvPVX(rv));
			// assert(strlen(SvPVX(rv)) == SvCUR(rv));
			// sv_dump(rv);
			RETURN_ST(rv);
		} else {
			RETURN_404;
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

void update_location(SV *, int id, int country, SV *distance, SV *city, SV *place )
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		Location * loc;
		std::map<int, Location*>::iterator it = (*self->locations).find(id);
		if ( it != (*self->locations).end() ) {
			loc = (*it).second;
		} else {
			warn("No location for update %d",id);
			XSRETURN_UNDEF;
		}

		if (country)
			loc->country = country;
		if (SvOK(distance))
			loc->distance = SvIV(distance);
		if (SvOK(city)) {
			SvREFCNT_dec(loc->city);
			loc->city = SvREFCNT_inc(city);
		}
		if (SvOK(place)) {
			SvREFCNT_dec(loc->place);
			loc->place = SvREFCNT_inc(place);
		}
		XSRETURN_UNDEF;

void exists_location(SV *, SV * id)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		if (SvOK(id)) {
			// int iid = SvIV(id);
			std::map<int, Location*>::iterator it = (*self->locations).find(SvIV(id));
			if ( it != (*self->locations).end() ) {
				// warn("Searching for %d -> %p", iid, (*it).second);
				ST(0) = &PL_sv_yes;
				XSRETURN(1);
				return;
			// } else {
				// warn("Searching for %d -> NOT FOUND", iid);
			}
		// } else {
			// sv_dump(id);
			// warn("Searching failed: %d/%d (%s)",SvOK(id),SvIOK(id),SvPV_nolen(id));
		}
		XSRETURN_UNDEF;

void get_location_rv(int id,SV *,SV *)
	PPCODE:
		register HLCup *self = DEFAULT;
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
			*p = 0;

			SvCUR_set(rv,p - SvPVX(rv));
			RETURN_ST(rv);
		}
		else {
			RETURN_404;
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

		Location *loc;
		std::map<int,Location*>::iterator location_it = (*self->locations).find(location);
		if ( location_it != (*self->locations).end() ) {
			loc = (*location_it).second;
		}
		else {
			croak("Failed to lookup location %d",(location));
		}

		User *usr;// = (*self->users)[vis->user];
		std::map<int,User*>::iterator user_it = (*self->users).find(user);
		if ( user_it != (*self->users).end() ) {
			usr = (*user_it).second;
		}
		else {
			croak("Failed to lookup user %d",(user));
		}

		// Location *loc = (*self->locations)[location];
		// User *usr = (*self->users)[user];
		if (loc && usr) {
			VisitSet *vs = new VisitSet;
			vs->visit = vis;
			vs->user = usr;
			vs->location = loc;

			(*self->visits)[id] = vis;
			usr->visits.insert(std::pair< int,VisitSet* >(vis->visited_at, vs));
			loc->visits.insert(std::pair< int,VisitSet* >(vis->visited_at, vs));
		}
		else{
			croak("XXX: No user or location");
		}
		XSRETURN_UNDEF;

void update_visit(SV *, int id, int user, int location, int mark, int visited_at )
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		std::map<int, Visit*>::iterator it = (*self->visits).find(id);
		Visit * vis;
		if ( it != (*self->visits).end() ) {
			vis = (*it).second;
		} else {
			XSRETURN_UNDEF;
			return;
		}
		// // 1. change only visited_at. move in user & location
		// // 2. change user. delete from old user, add in new
		// // 3. change location. delete from old loc, add in new

		if (mark > -1)
			vis->mark = mark;

		Location *loc;
		std::map<int,Location*>::iterator location_it = (*self->locations).find(vis->location);
		if ( location_it != (*self->locations).end() ) {
			loc = (*location_it).second;
		}
		else {
			croak("Failed to lookup location %d",(vis->location));
		}

		User *usr;// = (*self->users)[vis->user];
		std::map<int,User*>::iterator user_it = (*self->users).find(vis->user);
		if ( user_it != (*self->users).end() ) {
			usr = (*user_it).second;
		}
		else {
			croak("Failed to lookup user %d",(vis->user));
		}

		VisitSet *vs;

		std::multimap<int, VisitSet*>::iterator loc_it;
		std::map<int, VisitSet*>::iterator usr_it;

		loc_it = loc->visits.find( vis->visited_at );
		if (loc_it != loc->visits.end()) {
			for (; loc_it != loc->visits.end(); loc_it++) {
				if ((*loc_it).second->visit->id == id) {
					vs = (*loc_it).second;
					loc->visits.erase(loc_it);
					// warn("found loc vs for update: %p", vs);
					break;
				}
			}
		}
		else {
			warn("Bullshit, visit %d not found in location", id);
		}

		usr_it = usr->visits.find( vis->visited_at );
		if (usr_it != usr->visits.end()) {
			for (; usr_it != usr->visits.end(); usr_it++) {
				if ((*usr_it).second->visit->id == id) {
					vs = (*usr_it).second;
					usr->visits.erase(usr_it);
					// warn("found usr vs for update: %p", vs);
					break;
				}
			}
		}
		else {
			warn("Bullshit, visit %d not found in user", id);
		}

		if (visited_at) {
			vis->visited_at = visited_at;
		}

		if (location) {
			vis->location = location;
			// loc = (*self->locations)[location];
			std::map<int,Location*>::iterator location_it = (*self->locations).find(location);
			if ( location_it != (*self->locations).end() ) {
				loc = (*location_it).second;
			}
			else {
				croak("Failed to lookup location %d",(vis->location));
			}

			vs->location = loc;
		}

		if (user) {
			vis->user = user;
			// usr = (*self->users)[user];
			std::map<int,User*>::iterator user_it = (*self->users).find(user);
			if ( user_it != (*self->users).end() ) {
				usr = (*user_it).second;
			}
			else {
				croak("Failed to lookup user %d",(user));
			}
			vs->user = usr;
		}
		usr->visits.insert(std::pair< int,VisitSet* >(vis->visited_at, vs));
		loc->visits.insert(std::pair< int,VisitSet* >(vis->visited_at, vs));

		XSRETURN_UNDEF;

void exists_visit(SV *, SV * id)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		if (SvOK(id)) {
			std::map<int, Visit*>::iterator it = (*self->visits).find(SvIV(id));
			if ( it != (*self->visits).end() ) {
				ST(0) = &PL_sv_yes;
				XSRETURN(1);
			}
		}
		XSRETURN_UNDEF;

void get_visit_rv(int id, SV *, SV *)
	PPCODE:
		register HLCup *self = DEFAULT;
		std::map<int, Visit*>::iterator it = (*self->visits).find(id);
		if ( it != (*self->visits).end() ) {
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
			*p = 0;

			SvCUR_set(rv,p - SvPVX(rv));
			// sv_dump(rv);
			RETURN_ST(rv);
		}
		else {
			RETURN_404;
		}

void get_location_avg(SV *, int id, int from, int till, int from_age, int till_age, char gender)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		Location *loc;
		std::map<int,Location*>::iterator location_it = (*self->locations).find(id);
		if ( location_it != (*self->locations).end() ) {
			loc = (*location_it).second;
		}
		else {
			RETURN_404;
		}

		std::multimap<int, VisitSet*>::iterator it;
		if (from) {
			it = loc->visits.lower_bound(from);
		}
		else {
			it = loc->visits.begin();
		}

		//warn("%d < visited_at < %d; %d < age < %d (%c)", from, till, from_age, till_age, gender);
		int sum = 0;
		int cnt = 0;

		for (; it != loc->visits.end(); ++it) {
			if ( (*it).second->visit->visited_at > till ) break;
			if ( (*it).second->user->birth_date <= from_age ) continue;
			if ( (*it).second->user->birth_date > till_age ) continue;
			if ( gender && (*it).second->user->gender != gender ) continue;
			sum += (*it).second->visit->mark;
			cnt++;
		}

		SV *rv = newSV(22);
		SvUPGRADE( rv, SVt_PV );
		SvPOKp_on(rv);
		char *p = SvPVX(rv);

		// {"avg":,}

		if (cnt > 0) {
			// int($sum/$cnt*1e5+0.5)/1e5
			// p += snprintf(p, 22, "{\"avg\":%.6g}\n", (double)sum/cnt+DBL_EPSILON);
			p += snprintf(p, 22, "{\"avg\":%.6g}\n", ((double)(int)((double)sum * 1e5 / cnt + 0.5))/1e5);
		} else {
			mycpy(p,"{\"avg\":0}\n");
		}
		*p = 0;
		SvCUR_set(rv,p - SvPVX(rv));
		RETURN_ST(rv);

void get_location_visits(SV *, int id, int from, int till, int from_age, int till_age, char gender)
	PPCODE:
		register HLCup *self = ( HLCup * ) SvIV( SvRV( ST(0) ) );
		Location *loc;
		std::map<int,Location*>::iterator location_it = (*self->locations).find(id);
		if ( location_it != (*self->locations).end() ) {
			loc = (*location_it).second;
		}
		else {
			XSRETURN_UNDEF;
		}

		std::multimap<int, VisitSet*>::iterator it;
		if (from) {
			it = loc->visits.lower_bound(from);
		}
		else {
			it = loc->visits.begin();
		}

		//warn("%d < visited_at < %d; %d < age < %d (%c)", from, till, from_age, till_age, gender);
		AV *rv = newAV();

		for (; it != loc->visits.end(); ++it) {
			if ( (*it).second->visit->visited_at > till ) break;
			if ( (*it).second->user->birth_date <= from_age ) continue;
			if ( (*it).second->user->birth_date > till_age ) continue;
			if ( gender && (*it).second->user->gender != gender ) continue;
			VisitSet *vs = (*it).second;
			HV * vh = newHV();
			hv_stores(vh,"id",newSViv( vs->visit->id ));
			hv_stores(vh,"mark",newSViv( vs->visit->mark ));
			hv_stores(vh,"user",newSViv( vs->user->id ));
			hv_stores(vh,"location",newSViv( vs->location->id ));
			hv_stores(vh,"birth_date",newSViv( vs->user->birth_date ));
			hv_stores(vh,"gender",newSVpvf( "%c",vs->user->gender ));
			av_push(rv,newRV_noinc((SV *)vh));
		}

		ST(0) = sv_2mortal(newRV_noinc((SV*)rv));
		XSRETURN(1);

void get_user_visits_rv(SV * id_sv, SV * prm_sv, SV *)
	PPCODE:
		int id = SvIV(id_sv);
		register HLCup *self = DEFAULT;

		int from = 0;
		int till = 2147483647;
		int country = 0;
		int distance = 0;
		if (SvRV(prm_sv)) {
			HV *prm = (HV *)SvRV(prm_sv);
			SV **key;
			if ((key = hv_fetchs(prm,"fromDate",0)) && SvOK(*key)) {
				if (looks_like_number(*key)) {
					from = SvIV(*key);
				}
				else {
					RETURN_400;
				}
			}
			if ((key = hv_fetchs(prm,"toDate",0)) && SvOK(*key)) {
				if (looks_like_number(*key)) {
					till = SvIV(*key);
				}
				else {
					RETURN_400;
				}
			}
			if ((key = hv_fetchs(prm,"toDistance",0)) && SvOK(*key)) {
				if (looks_like_number(*key)) {
					distance = SvIV(*key);
				}
				else {
					RETURN_400;
				}
			}
			if ((key = hv_fetchs(prm,"country",0))) {
				if (SvOK(*key)) {
					country = get_country(self, *key);
				}
				else {
					RETURN_400;
				}
			}
		}

		std::map<int,User*>::iterator find = (*self->users).find(id);
		User *usr;
		if ( find != (*self->users).end() ) {
			usr = (*find).second;
		}
		else {
			RETURN_404;
		}
		
		std::multimap<int, VisitSet*>::iterator it;
		if (from) {
			it = usr->visits.lower_bound(from);
		}
		else {
			it = usr->visits.begin();
		}
		
		// warn("user_visits: %d < visited_at < %d; country: %d, distance: %d for <%s>", from, till, country, distance, SvPVX(usr->email));
		
		SV *rv = sv_2mortal(newSV(1024));
		SvUPGRADE( rv, SVt_PV );
		SvPOKp_on(rv);
		sv_catpv(rv,"{\"visits\":[\n");
		bool first = true;
		
		for (; it != usr->visits.end(); ++it) {
			if ( (*it).second->visit->visited_at > till ) break;
			if ( country && (*it).second->location->country != country ) continue;
			if ( distance && (*it).second->location->distance >= distance ) continue;
			Visit *vis = (*it).second->visit;
			Location *loc = (*it).second->location;
			if (!first) {
				sv_catpv(rv,",\n\t{\"mark\":");
			}
			else {
				sv_catpv(rv,"\t{\"mark\":");
				first = false;
			}
			sv_catpvf(rv, "%d", vis->mark);
			sv_catpvf(rv, ",\"visited_at\":%d",vis->visited_at);
			sv_catpv(rv,",\"place\":\"");
			sv_catpvn(rv, SvPVX(loc->place),SvCUR(loc->place));

			// sv_catpvf(rv, "\",\"extra\":\"loc:%d",loc->id);

			sv_catpv(rv,"\"}");
		}
		sv_catpv(rv,"\n]}\n");
		RETURN_ST(rv);















