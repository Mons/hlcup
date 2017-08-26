use 5.016000;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Local::HLCup',
    VERSION_FROM      => 'lib/Local/HLCup.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Local/HLCup.pm', # retrieve abstract from module
       AUTHOR         => 'Mons <mons@apple.com>') : ()),
    LIBS              => ['-lstdc++'], # e.g., '-lm'
    DEFINE            => '', # e.g., '-DHAVE_SOMETHING'
    INC               => '-I.', # e.g., '-I. -I/usr/include/other'

    CC       => 'g++',
    # LD       => '$(CC)',
    XSOPT    => '-C++',

    # TYPEMAPS => ['perlobject.map'],


	# Un-comment this if you add C files to link with later:
    # OBJECT            => '$(O_FILES)', # link all the C files too
    # OBJECT            => 'HLCup.o rb_tree.o',
);
