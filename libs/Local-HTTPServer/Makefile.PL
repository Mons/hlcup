use 5.016;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Local::HTTPServer',
    VERSION_FROM      => 'lib/Local/HTTPServer.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ABSTRACT_FROM     => 'lib/Local/HTTPServer.pm', # retrieve abstract from module
    AUTHOR            => 'Mons <mons@cpan.org>',
    LIBS              => [''], # e.g., '-lm'
    DEFINE            => '', # e.g., '-DHAVE_SOMETHING'

    CONFIGURE => sub {
        require EV::MakeMaker;
        return {EV::MakeMaker::ev_args(
            INC               => '-I./xsmy -I.',
        )};
    },
    # INC               => '-I.', # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
    OBJECT            => 'HTTPServer.o picohttpparser.o', # link all the C files too
);
