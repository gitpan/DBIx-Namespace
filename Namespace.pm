package DBIx::Namespace;
our $VERSION = 0.03;
use strict;
use warnings;
use DBI qw(:DEFAULT :sql_types);
use Carp;

# Global constants
our $toptable = 0;
our $nspc_root = "i$toptable";

=head1 NAME

DBIx::Namespace - Provide directory-like table names

=head1 ABSTRACT

One of the limitations of SQL is that the tables lie within a flat namespace.  This module provides
a directory-like partitioning, allowing identical table names to co-exist within the same database.

=head1 SYNOPSIS

    use DBIx::Namespace;
    
    $db = new DBIx::Namespace(
	    dbsource => 'dbi:mysql:',
	    database => 'a_database',
	    user     => 'a_user',
	    password => 'a_password',
	);
    
    $db->create($table, $sql_description);
    $db->delete($table);
    
    $db->replace($table, %field_data);
    my $ar = $db->select($table, 'name, address',
	'where country = ?', 'UK');
    my $hr = $db->select_hash($table, 
	'where id = ?', $id);
    my $phone = $db->select_one($table, 'phone',
	'where name = ?', 'Jim'); 
    $db->delete($table, 'where id = ?', $id);

    my $sql_name = $db->table($namespace_name);
    my $dbh = $db->dbh();
	   
=head1 DESCRIPTION

This module provides more flexiblity in naming tables than SQL provides by default.  They are identified using
a '::' separated naming structure similar to Perl module names, e.g. 'my::deeply::nested::sql::table'.

These user names are mapped to SQL table names using index tables stored in the database.  There is, therefore,
a small speed penalty in using this module as an extra lookup is needed for each name partition.  The index tables are
of the form 'i<integer>' while user data is stored in tables named 't<integer>'.  If these name patterns are avoided,
there is no reason why this heirarchical name structure should not co-exist alongside standard table names.

=head1 CONSTRUCTOR

=cut

sub new {
    my $class = shift;
    my $opt = {};
    if (ref($_[0]) eq 'HASH') { $opt = $_[0]; } else { %$opt = @_; }
   
    my $o = {};
    bless( $o, $class );

    $opt->{user}     = $ENV{DBI_USER}	unless defined $opt->{user};
    $opt->{password} = $ENV{DBI_PASS}	unless defined $opt->{password};
    $opt->{database} = 'test'           unless defined $opt->{database};
    $opt->{dbsource} = "DBI:mysql:"	unless defined $opt->{dbsource};
    croak "User required" unless $opt->{user};
    croak "Password required" unless $opt->{password};
    $o->{dbh} = DBI->connect($opt->{dbsource}, $opt->{user}, $opt->{password}, {RaiseError => 1, PrintError => 0})
	or croak "Cannot connect to '$opt->{dbsource}'\n";
    
    my @databases = $o->sql_show("databases");
    $o->{dbh}->do("create database $opt->{database}") unless search_array(\@databases, $opt->{database});
    $o->{dbh}->do("use $opt->{database}");

    return $o;
}

=head2 new( [options] )

A connection is made to the mysql server and the specified database is selected for use.  An exeption will be
thrown if an error is encountered.  C<options> may be either a hash ref or a list of hash keys and values.
Recognized keys are as follows.

=over 8

=item dbsource

This is the DBD driver string string passed to DBI::connect().  It depends on the database driver being used.  For
mysql it would be of the form:

    'DBI:mysql:host=localhost;port=3306;database=test'

Specifying a database is sometimes required.  However, passing it as a seperate option enables it to be created if
it does not yet exist, assuming the user has adequate permissions.  The minimal string would be:

    'DBI:mysql:'
    
=item user

The database user that is logging on.  The environment variable DBI_USER may be used instead.

=item password

The user's password.  The environment variable DBI_PASS may be used instead.  If neither is provided, an attempt
is made to read a password from the console.

=item database

The name of the database to be used.

=back

=cut

sub DESTROY {
    my ($o) = @_;
    $o->{dbh}->disconnect() if $o->{dbh};
}

=head1 MAIN METHODS

=cut

sub create {
    my ($o, $name, $description) = @_;
   
    unless ($o->sql_exists($nspc_root)) {
	my $job = "create table i$toptable ";
	$job   .= "( username varchar(255) not null, sqlname varchar(255), primary key (username) )";
	$o->{dbh}->do($job);
	$o->sql_replace($nspc_root, username => ' latest', sqlname => $toptable);
    }

    my ($index, $count, @count) = $nspc_root;
    my @tables  = split('::', $name);
    for (my $i = 0; $i < @tables; $i++) {
	my $uname = $tables[$i];
	my @found = $o->sql_select(qq(sqlname from $index where username = ?), $uname);
	if (@found) {
	    $index = $found[0][0];
	    if ($index =~ /t(\d+)/) {
		$count = $1;
	    }
	} else {
	    # update counter
	    @count = $o->sql_select(qq(sqlname from $nspc_root where username = ?), ' latest');
	    $count = ++($count[0][0]);
	    $o->sql_replace($nspc_root, username => ' latest', sqlname => $count);
	    # enter index entry
	    if ($i == $#tables) {
		$o->sql_replace($index, username => $uname, sqlname => "t$count");
	    } else {
		$o->sql_replace($index, username => $uname, sqlname => "i$count");
		my $job = "create table i$count ";
		$job   .= "( username varchar(255) not null, sqlname varchar(255), primary key (username) )";
		$o->{dbh}->do($job);
		$index = "i$count";
	    }
	}
    }
    
    # create user table
    my $table = "t$count";
    $o->{dbh}->do("drop table $table") if ($o->sql_exists($table));
    $o->{dbh}->do("create table $table ( $description )");

    return $table;
}
# Index names begin with i
# Data tables begin with t
# index and found[0][0] include this prefix
# count and count[0][0] are the plain counter

=head2 create( name, description )

=over 8

=item name

A user defined name for the table to be created.  It may be a compound name made of '::' seperated tokens, like
Perl package names.

=item description

A string of comma seperated mysql field and index definitions.  See the mysql documentation for more details (esp.
'CREATE TABLE Syntax' and 'Column Types').

=back

Creates an empty mysql table for C<name> with the SQL specificion given.  Returns the SQL name of the table created.

Example

    $db->create('table_name', q(
	userID INT(4) NOT NULL AUTO_INCREMENT,
	serialNr INT(8) NOT NULL,
	email VARCHAR(80),
	PRIMARY KEY (userID),
	UNIQUE (serialNr)
    ));

=cut

sub delete {
    my ($o, $name, $where, @values) = @_;
    
    my $uname;
    my $iname = '';
    my $index  = $nspc_root;
    my @sqltbl = ([$index]);
    if ($name) {
	my @tables = split('::', $name);
	foreach $uname (@tables) {
	    $index = $sqltbl[0][0];
	    if ($index and $o->sql_exists($index)) {
		@sqltbl = $o->sql_select(qq(sqlname from $index where username = ?), $uname);
		$iname = $uname;
	    }
	}
    }
    my $tbl = $sqltbl[0][0];
    croak "Unable to delete '$name', the SQL table is missing" unless ($tbl and $iname);
    if ($where) {
	$o->{dbh}->do("delete from $tbl where $where", undef, @values);
    } else {
	$o->{dbh}->do("delete from $index where username = ?", undef, $iname);
	$o->{dbh}->do("drop table $tbl");
    }
}

=head2 delete( table [, where [, values...] )

=over 8

=item table

The user name of the table containing the record to be deleted.

=item where

A where clause identifying the row(s) to be deleted.

=item values

Zero or more values to fill any '?' placeholders specified in the C<where> clause.

=back

One or more rows are deleted from the specified table.  An exception is raised if an problem is encountered.

=cut
sub replace {
    my $o = shift;
    my $table = $o->table(shift);
    return $o->sql_replace($table, @_);   
}


=head2 replace( table, data... )

An extremely useful method as it ensures the data is stored even if only a single field in the C<data> hash has
changed.

=over 8

=item table

A Namespace table name, case sensitive.  Use B<sql_replace> if you have the SQL table name.
    
=item data

All subsequent arguments should be in hash 'field => value' format.  As well as entering these directly, C<data>
can also be a prefilled array or hash.

=back

Note that this uses the SQL REPLACE ... SET variant, and that any fields not specified in C<data> will probably be
set to NULL.  This can be overcome with a prior call to B<select_hash>.

Example

    my $db    = new DBIx::Namespace(...);
    my $table = 'addresses::my::family';
    my $hr    = $db->select_hash( $table,
		    'surname = ? and forename = ?',
		    'Smith', 'John'
		);
    $hr->{telephone} = '0191428991';
    $db->replace( $table, %$hr );

The telephone number has been changed and the rest of the data remain as they were.
    
=cut


sub select {
    my ($o, $table, $columns, $clause, @values) = @_;
    my $tbl = $o->table($table);
    $clause = '' unless defined $clause;
    my $r = $o->{dbh}->selectall_arrayref("select $columns from $tbl $clause", undef, @values);
    return $r ? @$r : ();    
}

=head2 select( name, columns [, clause [, values...]] )

Perform a SQL SELECT query.

=over 8

=item name

The Namespace name identifying the table to be searched.

=item columns

A comma seperated list of field names.

=item clause

The rest of the SELECT statement, typically beginning 'WHERE...'.  If it includes any '?'
placeholders, the corresponding number of values should be passed.

Although used most of the time, the 'WHERE...' clause is optional.  For example, this might begin 'ORDER BY...',
so the WHERE keyword cannot be added by the method.   To avoid confusion it is also required by the other
B<select> variants.

=item values

Zero or more strings or numbers matching the '?' placeholders in C<clause>.

=back

In list context returns a list of arrayrefs, one for each record.  In scalar context the number of rows is
returned.  An exception is thrown if an error is encountered.

If this is too limiting, B<sql_select> allows complete flexibility.  A call to B<table> will be required to
obtain the SQL table name needed.

=cut

sub select_hash {
    my ($o, $table, $clause, @values) = @_;
    my $tbl = $o->table($table);
    croak 'A where clause is required' unless $clause;
    my $r = $o->{dbh}->selectrow_hashref("select * from $tbl $clause", undef, @values);
    return $r;    
}

=head2 select_hash( name , clause [, values...]] )

Perform a SQL SELECT query to obtain all fields in a single record.

=over 8

=item name

The Namespace name identifying the table to be searched.

=item clause

The rest of the SELECT statement (beginning "WHERE...").  If it includes any '?' placeholders, the
corresponding number of values should be passed.

The keyword 'WHERE' is required as part of the argument to match the B<select> method.

=item values

Zero or more strings or numbers matching the '?' placeholders in C<clause>.

=back

Returns a reference to a hash keyed by field names.

=cut

sub select_one {
    my ($o, $table, $columns, $clause, @values) = @_;
    my $tbl = $o->table($table);
    croak 'At least one column name is required' unless $columns;
    croak 'A where clause is required' unless $clause;
    return $o->{dbh}->selectrow_array("select $columns from $tbl $clause", undef, @values);
}

=head2 select_one( name, column, clause [, values...]] )

Perform a SQL SELECT query to obtain particular field value(s).

=over 8

=item name

The Namespace name identifying the table to be searched.

=item column

The column name (or a string of comma seperated names) required.

=item clause

The rest of the SELECT statement, beginning "WHERE...".  If it includes any '?' placeholders, the
corresponding number of values should be passed.

The keyword 'WHERE' is required as part of the argument to match the B<select> method.

=item values

Zero or more strings or numbers matching the '?' placeholders in C<clause>.

=back

In list context, the requested fields of the first matching row are returned.  In scalar context, only the first
field is returned.

=cut

=head1 SUPPORT METHODS

All methods beginning 'sql_' work on SQL table names, not the Namespace names used by the MAIN METHODS.

=cut

sub dbh {
    return shift->{dbh};
}

=head2 dbh()

Return a handle to the underlying DBI object.

=cut

sub disconnect {
    my ($o) = @_;
    $o->{dbh}->disconnect() if $o->{dbh};
    $o->{dbh} = undef;
}

=head2 disconnect()

Manual disconnection.  Not usually required as this happens automatically once the DBIx::Namespace object is
finished with.

=cut

sub quote {
    my ($o, $val) = @_;
    return $o->{dbh}->quote($val);
}

=head2 quote( value )

Quote a string for passing to mysql.  Any embedded quotes are suitably escaped as well.

=cut

sub root {
    return $nspc_root;
}

=head2 root()

Return the SQL name of the root table.

=cut

sub table {
    my ($o, $name) = @_;
    my @sqltbl = ([$nspc_root]);
    if ($name) {
	my @tables = split('::', $name);
	foreach my $uname (@tables) {
	    my $tbl = $sqltbl[0][0];
	    croak "No SQL table for '$name'" unless ($tbl and $o->sql_exists($tbl));
	    @sqltbl = $o->sql_select(qq(sqlname from $sqltbl[0][0] where username = ?), $uname);
	}
    }
    my $tbl = $sqltbl[0][0];
    croak "No SQL table for '$name'" unless ($tbl and $o->sql_exists($tbl));
    return $tbl;
}

=head2 table( name )

This looks up the user table name given and returns the SQL table name.  If C<name> is omitted, the root table
is returned.  An exception is thrown if the table doesn't exist.

=cut

sub sql_describe {
    my($o, $table, $column) = @_;
    $column = '' unless defined $column;
    my $array = $o->{dbh}->selectall_arrayref("describe $table $column");
    return () unless @$array;
    return @$array if wantarray;
    my %h;
    foreach my $row (@$array) {
	my $key = $row->[0];
	$h{$key}{type}    = $row->[1];
	$h{$key}{null}    = $row->[2];
	$h{$key}{key}     = $row->[3];
	$h{$key}{default} = $row->[4];
	$h{$key}{extra}   = $row->[5];
    }
    return \%h;
}

=head2 sql_describe( table [, column] )

In list context returns a list of array refs.  
Each array item describes the field structure of the mysql table given:

    [ field, type, null, key, default, extra ]

In scalar context, this is converted to a hash of hashes keyed initially by 'field'.

=over 8

=item field

The field name.

=item type

The data format e.g. 'int(11)', 'date' or 'varchar(255)'.

=item null

'yes' if the field can be null.

=item key

Whether the field is a key field, and whether primary or secondary.

=item default

The default value, if any.

=item extra

Other stuff?  I haven't come across a use for this.

=back

If C<column> is given, only the data for that column is returned.  For example, after

    my $h = $db->sql_describe('table', 'address');

$h->{$address}{type} might be 'varchar(80)'.

=cut

sub sql_eval {
    my ($o, $query, @values) = @_;
    my @res = $o->{dbh}->selectrow_array("select $query", undef, @values);
    return wantarray() ? @res : $res[0];
}

=head2 sql_eval( command [, values...] )

Evaluate a SQL SELECT command.  Called in scalar context it returns a single value.

Although intended for evaluating SQL functions that evaluate without accessing any tables, it will return a single
record if called in an array context.  Tables must be SQL names and C<values> are needed for any '?' placeholders.
The SELECT statement is not required.

Examples

    my $day = $db->sql_eval( 'dayname(now())' );
    my $day = $db->sql_eval( 'dayname(?)', $date );

=cut

sub sql_exists {
    my ($o, $table) = @_;
    croak "No table specified" unless $table;
    my $tables = $o->{dbh}->selectall_arrayref("show tables");
    return search_array($tables, $table) ? 1 : 0;
}

=head2 sql_exists( table )

Return 1 if the named table exists in the database, 0 if it does not.  Note that C<table> is a SQL table name and
therefore case sensitive.

=cut

sub sql_names {
    my ($o, $table, $prefix, $level) = @_;
    $table  = $nspc_root unless defined $table;
    $prefix = '' unless defined $prefix;
    $level  = 1  unless defined $level;
    my @rows = $o->sql_select(qq(username, sqlname from $table));

    my @names;
    foreach my $row (@rows) {
	my ($username, $sqlname) = @$row;
	push @names, [ "${prefix}${username}", $sqlname, $level ];
	if ($sqlname =~ /^i/) {
	    push @names, $o->sql_names( $sqlname, "${prefix}${username}::", $level+1 );
	}
    }
    return @names;
}

=head2 sql_names( [table [, prefix, level] )

Output the mapping of Namespace names and their corresponding SQL table names.

=over 8

=item table

An optional mysql table name to start with.  If omitted, the whole tree is assumed.

=item prefix

Used in recursive calls for building names.

=item level

Used in recursive calls for indenting output.

=back

=back

Returns a list of arrayrefs each of the following form.

    [ Namespace_name, SQL_table, nesting_level ]

The nesting_level is included for printing indented output, e.g. 

    print '  ' x $nesting_level, $name, "\n";

=cut

sub sql_replace {
    my $o = shift;
    my $table = shift;
    my $set = '';
    my @values;
    for (my $i = 0; $i < $#_; $i += 2) {
	my $v = $_[$i+1];
	if (defined $v and not $v eq '') {
	    $set    .= (($set    ? ", " : "") . "$_[$i] = ?");
	    push @values, $v;
	}
    }
    
    return $o->{dbh}->do("replace into $table set $set", undef, @values);
}

=head2 sql_replace( table, data... )

=over 8

=item table

The SQL table name, case sensitive.  Note that this is not the user name for the table.
    
=item data

All subsequent arguments should be in hash 'field => value' format.  As well as entering these directly, C<data>
can also be a prefilled array or hash.

Note that this uses the SQL REPLACE ... SET variant, and that any fields not specified in C<data> will probably be
set to NULL.

=cut

sub sql_select {
    my ($o, $query, @values) = @_;
    my $r = $o->{dbh}->selectall_arrayref("select $query", undef, @values);
    return $r ? @$r : ();    
}

=head2 sql_select( query [, values...] )

Perform a SQL SELECT query.  C<query> should be everything after the SELECT keyword.  If it includes any '?'
placeholders, the corresponding number of values should be passed.

In list context returns a list of arrayrefs, one for each record.  In scalar context the number of rows is
returned.  An exception is thrown if an error is encountered.

=cut

sub sql_show {
    my ($o, $query, @values) = @_;
    my $r = $o->{dbh}->selectall_arrayref("show $query", undef, @values);
    return $r ? @$r : ();    
}

=head2 sql_show( query [, values...] )

Perform a SQL SHOW query.  C<query> should be everything after the SHOW keyword.  If it includes any '?'
placeholders, the corresponding number of values should be passed.

In list context returns a list of arrayrefs, one for each record.  In scalar context the number of rows is
returned.  An exception is thrown if an error is encountered.

=cut

### Private methods

sub search_array ($$;$) {
    my ($ar, $value, $idx) = @_;
    $idx = 0 unless $idx;
    foreach my $rr (@$ar) {
	return @$rr if ($rr->[$idx] eq $value);
    }
    return ();
}

=head1 BUGS

Indexes are not removed when they become empty.

=head1 AUTHOR

Chris Willmot, chris@willmot.org.uk

=head1 SEE ALSO

L<Finance::Shares::MySQL> uses this as a base class.
L<Finance::Shares::CGI> make use of this directly.

=cut

1;

