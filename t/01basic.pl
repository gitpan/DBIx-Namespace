#!/usr/bin/perl
use strict;
use warnings;
use DBIx::Namespace;

 sub print_names;
 sub print_table;

my $db = new DBIx::Namespace( 
	user     => 'tester',
	password => 'tester',
	database => 'test',
    );

# creating a top level named table
my $table1 = 'TopLevel';
$db->create($table1, q(name char(20) not null, age int(4), sex enum('M', 'F'), primary key(name) ));
my $tbl = $db->table($table1);
print "$table1 created as $tbl\n";

# creating a nested namespace table
my $table2 = 'Some::Nested::Table';
$db->create($table2, q(name char(20) not null, age int(4), sex enum('M', 'F'), primary key(name) ));
$tbl = $db->table($table2);
print "$table2 created as $tbl\n";

# adding and retrieving rows
$db->replace($table1, name => 'Michael', age => 17, sex => 'M');
$db->replace($table1, name => 'David', age => 16, sex => 'M');
$db->replace($table1, name => 'Julia', age => 11, sex => 'F');
my @rows = $db->select($table1, 'name, age, sex');
print_table( \@rows, [qw(name age sex)] );
print "Replacing rows...\n";
my $hr = $db->select_hash($table1, 'where name = ?', 'Michael');
$hr->{age} = 18;
$db->replace($table1, %$hr);
@rows = $db->select($table1, 'name, age, sex');
print_table( \@rows, [qw(name age sex)] );

# deleting the nested namespace table
print_names();
$tbl = $db->table($table1);
print "Deleting David...\n";
$db->delete($table1, 'name = ?', 'David');
@rows = $db->select($table1, 'name, age, sex');
print_table( \@rows, [qw(name age sex)] );

# deleting the top level named table
$tbl = $db->table($table2);
$db->delete($table2);
print "Deleting $table2 ($tbl)\n";
print_names();

# creating another nested namespace table
my $table3 = 'Some::Nested::Chair';
$db->create($table3, q(name char(20) not null, age int(4), sex enum('M', 'F'), primary key(name) ));
$tbl = $db->table($table3);
print "$table3 created as $tbl\n";
print_names();

# deleting the top level named table
$tbl = $db->table($table3);
$db->delete($table3);
print "Deleting $table3 ($tbl)\n";
$tbl = $db->table($table1);
$db->delete($table1);
print "Deleting $table1 ($tbl)\n";
print_names();

sub print_names {
    my @rows = $db->sql_names();
    print "Namespace entries...\n";
    foreach my $r (@rows) {
	my ($name, $sql, $level) = @$r;
	printf '%5s%s%s%s', $sql, '  ' x $level, $name, "\n";
    }
    print "---\n";
}
    
sub print_table {
    my ($rows, $cols) = @_;
    if ($rows) {
	my $gap = 10;
	if ($cols) {
	    foreach my $head (@$cols) {
		printf("%*s ", $gap, $head);
	    }
	    print "\n";
	}
	foreach my $row (@$rows) {
	    foreach my $item (@$row) {
		printf("%*s ", $gap, $item || '');
	    }
	    print "\n";
	}
    }
}

