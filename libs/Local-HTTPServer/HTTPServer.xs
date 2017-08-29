#define PERL_NO_GET_CONTEXT
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

#include "picohttpparser.h"

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

typedef struct HTSCnn {
	ev_io    rw;
	ev_io    ww;
	struct ev_loop * loop;
	int      fd;
	SV     * self;
	SV     * cb;
	char     rbuf[32768];
	int      ruse;
	struct   phr_header headers[100];
	char     wbuf[32768];
} HTSCnn;

#define dSELFby(TYPE,ptr,xx) TYPE self = (TYPE) ( (char *) ptr - (ptrdiff_t) &((TYPE) 0)-> xx );

#define header_eq(h,s) (h.name_len==strlen(s) && strncasecmp(h.name, s, strlen(s)) == 0)


static inline int nonblocking(int fd) {
	int flags;
	if ((flags = fcntl(fd, F_GETFL, 0))>-1 ) {
		if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0) {
			return 0;
		}
		else {
			return -1;
		}
	}
	else {
		return -1;
	}
	
}

static
size_t find_ch(const char* s, size_t len, char ch)
{
  size_t i;
  for (i = 0; i != len; ++i, ++s)
    if (*s == ch)
      break;
  return i;
}

static inline
int hex_decode(const char ch)
{
  int r;
  if ('0' <= ch && ch <= '9')
    r = ch - '0';
  else if ('A' <= ch && ch <= 'F')
    r = ch - 'A' + 0xa;
  else if ('a' <= ch && ch <= 'f')
    r = ch - 'a' + 0xa;
  else
    r = -1;
  return r;
}
 
static SV * url_decode(const char * s, size_t len) {
	dTHX;
	char *dbuf, *d;
	size_t i;
   
	for (i = 0; i < len; ++i)
		if (s[i] == '%')
			goto NEEDS_DECODE;
	return newSVpvn(s,len);
   
	NEEDS_DECODE: {
		int hi, lo;
		SV *rv = newSV(len);
		SvUPGRADE(rv, SVt_PV);
		SvPOKp_on(rv);
		SvUTF8_on(rv);
		char *dbuf = SvPVX(rv);
		memcpy(dbuf, s, i);
		d = dbuf + i;
		while (i < len) {
			if (s[i] == '%' && (hi = hex_decode(s[i + 1])) > -1 && (lo = hex_decode(s[i + 2])) > -1 ) {
				*d++ = hi * 16 + lo;
				i += 3;
			} else {
				*d++ = s[i++];
			}
		}
		*d = '\0';
		SvCUR_set(rv, d-SvPVX(rv));
		return rv;
	}
}

#define NIBBLE_BITS 4
#define MAKE_BYTE(nh, nl) (((nh) << NIBBLE_BITS) | (nl))

static char uri_decode_tbl[256] =
/*    0    1    2    3    4    5    6    7    8    9    a    b    c    d    e    f */
{
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 0:   0 ~  15 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 1:  16 ~  31 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 2:  32 ~  47 */
      0,   1,   2,   3,   4,   5,   6,   7,   8,   9,   0,   0,   0,   0,   0,   0,  /* 3:  48 ~  63 */
      0,  10,  11,  12,  13,  14,  15,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 4:  64 ~  79 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 5:  80 ~  95 */
      0,  10,  11,  12,  13,  14,  15,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 6:  96 ~ 111 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 7: 112 ~ 127 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 8: 128 ~ 143 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* 9: 144 ~ 159 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* a: 160 ~ 175 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* b: 176 ~ 191 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* c: 192 ~ 207 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* d: 208 ~ 223 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* e: 224 ~ 239 */
      0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,  /* f: 240 ~ 255 */
};

static SV *decode_query(char *p, size_t len) {
	dTHX;
	HV *query_hash = newHV();
	char *e = p+len;
	char *nxt;
	do {
		char *nxt = strchr(p,'&');
		if (!nxt) nxt = e;
		char *eq = strchr(p,'=');
		if (!eq || eq > nxt) {
			(void)hv_store(query_hash,p,nxt-p,&PL_sv_undef,0);
		}
		else {
			SV *v = newSV(nxt-eq+2);
			SvUPGRADE(v, SVt_PV);
			SvPOKp_on(v);
			SvUTF8_on(v);
			char *pv = SvPVX(v);
			char *dp = eq+1; // data pointer
			while (dp < nxt) {
				if (*dp == '%' && dp+2 < nxt && isxdigit(dp[1]) && isxdigit(dp[2]) ) {
					*pv++ = MAKE_BYTE(uri_decode_tbl[dp[1]], uri_decode_tbl[dp[2]]);
					dp += 3;
				}
				else if(*dp == '+') {
					*pv++ = ' ';
					dp++;
				}
				else {
					*pv++ = *dp++;
				}
			}
			*pv = 0;
			SvCUR_set(v, pv - SvPVX(v));
			(void)hv_store(query_hash,p,eq-p,v,0);
		}
		p = nxt+1;
	} while (p < e);
	return newRV_noinc((SV *)query_hash);
}

// const char reply_header[] =
// "HTTP/1.1 000 X\015\012"
// "Server: Perl/5\015\012"
// "Connection: keep-alive\015\012"
// "Content-Length: "
// ;
// const size_t oft_status = strstr(reply_header,"000")-reply_header;
// const size_t oft_connection = strstr(reply_header,"Connection:")-reply_header+strlen("Connection: ");
// const size_t oft_content_len = sizeof(reply_header)-1;

static void free_conn( HTSCnn * self ) {
	ev_io_stop(self->loop,&self->rw);
	close(self->fd);
	safefree(self);
}
static void send_reply( HTSCnn * self, int status, char * body, size_t body_size, int close ) {
	// memcpy(self->wbuf,reply_header,sizeof(reply_header));
	// self->wbuf[oft_status]   = '0' + status % 1000 / 100;
	// self->wbuf[oft_status+1] = '0' + status % 100 / 10;
	// self->wbuf[oft_status+2] = '0' + status % 10 ;
	// if (close) {
	// 	memcpy(&self->wbuf[oft_connection], "     close", 10);
	// }
	// else {
	// 	memcpy(&self->wbuf[oft_connection], "keep-alive", 10);
	// }
	// char *p = &self->wbuf[oft_content_len];
	// int wr = sprintf(p, "%u", body_size);
	// memcpy(&self->wbuf[oft_content_len+wr],"\015\012\015\012", 4);
	int size = snprintf(self->wbuf, sizeof(self->wbuf)-1,
		"HTTP/1.1 %03d X\015\012"
		"Server: Perl/5\015\012"
		"Connection: %s\015\012"
		"Content-Length: %u\015\012\015\012%-.*s",
		status,
		close ? "close" : "keep-alive",
		body_size,
		body_size,body
	);
	// warn("%s",self->wbuf);
	int written = write(self->fd,self->wbuf, size);
	if (written < size) {
		warn("Write failed: %d of %d", written, size);
	}
	if (close) {
		free_conn(self);
	}
	else {
		ev_io_start( self->loop, &self->rw );
	}
}

static void on_cnn_read( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(HTSCnn*,w,rw);
	dTHX;

	// warn("Call read %p -> %p from %d (%d:%d)", w, self, self->fd, O_NONBLOCK, fcntl(self->fd, F_GETFL, 0) );
	int rc = read(self->fd, &self->rbuf[self->ruse], sizeof(self->rbuf)-self->ruse );
	// warn("read: %d (%s)",rc,strerror(errno));

	char *method, *path;
	int pret, minor_version;
	struct phr_header headers[100];
	size_t buflen = 0, prevbuflen = 0, method_len, path_len, num_headers, question_at;
	ssize_t rret;
	int i;
	int content_length;


	if (rc > 0) {
		prevbuflen = self->ruse;
		self->ruse += rc;
		ev_io_stop(loop,w);
		// warn("Received: '%-.*s'\n",self->ruse, self->rbuf);

		num_headers = sizeof(headers) / sizeof(headers[0]);
		pret = phr_parse_request(
			self->rbuf, self->ruse,
			&method, &method_len,
			&path, &path_len,
			&minor_version,
			headers, &num_headers,
			prevbuflen
		);
		if (pret > 0) {
			/* successfully parsed the request */

			// printf("request is %d bytes long\n", pret);
			// printf("method is %.*s\n", (int)method_len, method);
			// printf("path is %.*s\n", (int)path_len, path);
			// printf("HTTP version is 1.%d\n", minor_version);
			// printf("headers:\n");
			for (i = 0; i != num_headers; ++i) {
				if (header_eq(headers[i], "Content-Length")) {
					content_length = atol(headers[i].value);
				}
				// 	(int)headers[i].name_len, headers[i].name,
				// 	(int)headers[i].value_len, headers[i].value
			}
			// warn("Content-Length: %d",content_length);
			if (self->ruse < pret + content_length) {
				if (sizeof(self->rbuf) < pret + content_length) {
					send_reply(self,413,"Big request\n",strlen("Big request\n"),1);
					return;
				}
				ev_io_start( loop, w );
				return; // want more
			}

			dSP;
			ENTER;SAVETMPS;

			SV *sv_body;
			if (content_length > 0) {
				// printf("Body: '%-.*s'\n", content_length, self->rbuf + pret);
				sv_body = sv_2mortal(newSVpvn(self->rbuf + pret, content_length));
			}
			else {
				sv_body = &PL_sv_undef;
			}
			self->ruse -= pret + content_length;
			assert(self->ruse < 0);
			if (self->ruse > 0)
				memmove(self->rbuf,self->rbuf+pret+content_length,self->ruse);

			SV *sv_met = sv_2mortal(newSVpvn(method, method_len));
			path_len = find_ch(path, path_len, '#');
			question_at = find_ch(path, path_len, '?');
			SV *sv_path = sv_2mortal(url_decode(path,question_at));
			// sv_dump(sv_path);
			SV *sv_query;
			if (question_at != path_len) {
				++question_at;
				sv_query = sv_2mortal(decode_query(path+question_at, path_len - question_at));
			}
			else {
				sv_query = &PL_sv_undef;
			}
			// sv_dump(query);

			PUSHMARK(SP);
			EXTEND(SP, 4);
			PUSHs( sv_met );
			PUSHs( sv_path );
			PUSHs( sv_query );
			PUSHs( sv_body );
			// PUSHs( sv_2mortal(newSVpvf("Request timed out")) );
			PUTBACK;
			
			int rv_cnt = call_sv( self->cb, G_ARRAY );
			
			SPAGAIN;

			SV *rv_status,*rv_body;
			int rv_close = 0;
			if (rv_cnt == 3) {
				rv_close  = POPi;
				rv_body   = POPs;
				rv_status = POPs;
				goto PASS;
			}
			else if (rv_cnt == 2) {
				rv_body   = POPs;
				rv_status = POPs;
				goto PASS;
			}
			warn("Bad number of ret: %d",rv_cnt);
			goto END;
			PASS: {
				// sv_dump(rv_status);
				// sv_dump(rv_body);
				size_t rv_len;
				send_reply(self, SvIV(rv_status), SvPV_nolen(rv_body), SvCUR(rv_body), rv_close);
			}
			END:
			PUTBACK;
			FREETMPS; LEAVE;

		}
		else if (pret == -1) {
			// close;
			send_reply(self, 400, "Malformed\n", strlen("Malformed\n"), 1);
		}
		else if (pret == -2) {
			ev_io_start( loop, w );
		}
	}
	else if ( rc != 0 ) {
		switch(errno){
			case EINTR:
			case EAGAIN:
				ev_io_start( loop, w );
				return;
			case ECONNRESET:
				if (self->ruse) {
					warn("connection failed: %s (%d), have %d buf", strerror(errno), errno, self->ruse);
				}
				free_conn(self);
				return;
			default:
				//ev_io_stop(loop,w);
				warn("connection failed while read [io]: %s (%d)", strerror(errno), errno);
				free_conn(self);
		}
	}
	else {
		if (self->ruse) {
			warn("EOF while have %d buffer\n", self->ruse);
		}
		ev_io_stop(loop,w);
		free_conn(self);
	}

}

static void on_accept_io( struct ev_loop *loop, ev_io *w, int revents ) {
	dSELFby(HTS*,w,rw);
	dTHX;
	struct sockaddr cl;
	int newfd;
	// if ( (newfd = accept4(self->fd, NULL, NULL, SOCK_NONBLOCK)) ) {}
	// warn("Accepting from %d", self->fd);
	if ( (newfd = accept(self->fd, NULL, NULL)) > -1 ) {
		// warn("client connected: %d",newfd);
		if (nonblocking(newfd)) {
			warn("O_NONBLOCK failed on client %d: %s", newfd, strerror(errno));
			close(newfd);
			return;
		}

		HTSCnn * cnn = (HTSCnn *) safemalloc( sizeof(HTSCnn) );
		memset(cnn, 0, sizeof(HTSCnn));
		// warn("Created connection %p", cnn);
		// cnn->self = sv_bless(newRV_noinc(newSViv(PTR2IV( self ))), self->cnnstash);
		cnn->fd = newfd;
		cnn->cb = self->cb;
		cnn->loop = self->loop;

		ev_io_init( &cnn->rw, on_cnn_read, cnn->fd, EV_READ );
		on_cnn_read(self->loop, &cnn->rw, EV_READ);
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

		if (nonblocking(s))
			croak("fcntl F_SETFL O_NONBLOCK failed: %s", strerror(errno));

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

