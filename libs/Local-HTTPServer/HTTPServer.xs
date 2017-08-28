// #define PERL_NO_GET_CONTEXT
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "ppport.h"


#include <sys/types.h>

#include <errno.h>
#include "EVAPI.h"
#include <fcntl.h>

#define _GNU_SOURCE
#include <sys/socket.h>

// #include <netdb.h>
// #include <stdio.h>
// #include <string.h>

typedef struct HTS {
	ev_io    rw;
	struct ev_loop * loop;

	int fd;
	struct sockaddr_in sa;

	SV * host;
	int  port;

	SV *cb;
	HV *cnnstash;
} HTS;

typedef struct HTSCN {
	ev_io    rw;
	ev_io    ww;
	int      fd;
	SV     * self;
	char     rbuf[32768];
	int      ruse;
} HTSCnn;

#define dSELFby(TYPE,ptr,xx) TYPE self = (TYPE) ( (char *) ptr - (ptrdiff_t) &((TYPE) 0)-> xx );

static void on_cnn_read( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(HTSCnn*,w,rw);
	warn("Call read %p -> %p from %d", w, self, self->fd);
	int rc = read(self->fd, self->rbuf[self->ruse], sizeof(self->rbuf)-self->ruse );
	warn("read: %zu (%s)",rc,strerror(errno));
	return;
	if (rc > 0) {
		self->ruse += rc;
		// if (self->on_read) {
		// 	self->on_read(self,rc);
		// 	if (self->ruse == self->rlen)
		// 		on_connect_reset(self, ENOBUFS,NULL);//ENOSPC
		// }
	}
	else if ( rc != 0 ) {
		switch(errno){
			case EINTR:
			case EAGAIN:
				ev_io_start( loop, w );
				return;
			default:
				//ev_io_stop(loop,w);
				warn("connection failed while read [io]: %s", strerror(errno));
				// on_connect_reset(self,errno,NULL);
		}
	}
	else {
		warn("connection failed while read [io]: EOF: %s", strerror(errno));
		// if (self->on_read)
		// 	self->on_read(self,0);
		//on_read(self,0);
		ev_io_stop(loop,w);
		// on_connect_reset(self,ECONNABORTED,NULL);
	}

}

static void on_accept_io( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(HTS*,w,rw);
	dSP;
	struct sockaddr cl;
	int newfd;
	// if ( (newfd = accept4(self->fd, NULL, NULL, SOCK_NONBLOCK)) ) {}
	warn("Accepting from %d", self->fd);
	if ( (newfd = accept(self->fd, NULL, NULL)) > -1 ) {
		warn("client connected: %d",newfd);
		int flags = fcntl(newfd, F_GETFL, 0);
		if (flags < 0) {
			warn("fcntl F_GETFL failed on client %d: %s", newfd, strerror(errno));
			close(newfd);
			return;
		}
		if (fcntl(newfd, F_SETFL, flags|O_NONBLOCK) != 0) {
			warn("fcntl F_SETFL O_NONBLOCK failed on client %d: %s", newfd, strerror(errno));
			close(newfd);
			return;
		}
		HTSCnn * cnn = (HTSCnn *) safemalloc( sizeof(HTSCnn) );
		memset(self, 0, sizeof(HTSCnn));
		warn("Created connection %p", cnn);
		cnn->self = sv_bless(newRV_noinc(newSViv(PTR2IV( self ))), self->cnnstash);

		warn("Init rw");
		ev_io_init( &cnn->rw, on_cnn_read, cnn->fd, EV_READ );
		warn("Call on_cnn_read");
		on_cnn_read(self->loop, &cnn->rw, EV_READ);
		// ev_io_start( cnn->loop, &cnn->rw );

	}
}

MODULE = Local::HTTPServer::Cnn		PACKAGE = Local::HTTPServer


MODULE = Local::HTTPServer		PACKAGE = Local::HTTPServer
PROTOTYPES: DISABLE
BOOT:
{
	I_EV_API ("Local::HTTPServer");
}

void new(SV *, SV  * host, int port, SV *cb)
	PPCODE:
		HTS * self = (HTS *) safemalloc( sizeof(HTS) );
		memset(self, 0, sizeof(HTS));
		ST(0) = sv_2mortal(sv_bless(newRV_noinc(newSViv(PTR2IV( self ))), gv_stashpv(SvPV_nolen(ST(0)), TRUE)));
		self->host = SvREFCNT_inc(host);
		self->cb = SvREFCNT_inc(cb);
		self->port = port;
		self->cnnstash = gv_stashpv("Local::HTTPServer:Cnn", TRUE);

		XSRETURN(1);

void listen(SV *, ...)
	PPCODE:
		register HTS *self = ( HTS * ) SvIV( SvRV( ST(0) ) );
		int s;
		int backlog = 1024;
		if (items > 1) {
			backlog = SvIV(ST(1));
		}
		s = socket(AF_INET, SOCK_STREAM, 0);
		if (!s) croak("socket failed: %s", strerror(errno));
		self->sa.sin_family = AF_INET;
		self->sa.sin_addr.s_addr = htons(INADDR_ANY);
		self->sa.sin_port = htons(self->port);
		self->loop = EV_DEFAULT;

		int yes = 1;
		if ( setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1 ) {
 			croak("setsockopt SO_REUSEADDR failed: %s", strerror(errno));
		}
		// TODO: other setsockopt's

		int flags = fcntl(s, F_GETFL, 0);
		if (flags < 0) {
			croak("fcntl F_GETFL failed: %s", strerror(errno));
		}
		if (fcntl(s, F_SETFL, flags|O_NONBLOCK) != 0) {
			croak("fcntl F_SETFL O_NONBLOCK failed: %s", strerror(errno));
		}

 		if (bind(s, (struct sockaddr *) &self->sa, sizeof(self->sa))) {
 			croak("bind failed: %s", strerror(errno));
 		}
    	if ( listen(s, backlog) ) {
    		croak("listen failed: %s", strerror(errno));
    	}

    	self->fd = s;

		XSRETURN_UNDEF;

void accept(SV *)
	PPCODE:
		register HTS *self = ( HTS * ) SvIV( SvRV( ST(0) ) );

		ev_io_init( &self->rw, on_accept_io, self->fd, EV_READ );
		ev_io_start( self->loop, &self->rw );

		XSRETURN_UNDEF;

