use 5.008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME	    => 'DBIx::Namespace',
    VERSION_FROM    => 'Namespace.pm',
    PREREQ_PM	    => { 
			    Carp => 0, 
			    DBI => 0
			}, # e.g., Module::Name => 1.1
    ABSTRACT_FROM   => 'Namespace.pm',
    AUTHOR	    => 'Christopher Willmot <chris@willmot.org.uk>',
);
