# Copyright (C) all contributors <meta@public-inbox.org>
# License: AGPL-3.0+ <https://www.gnu.org/licenses/agpl-3.0.txt>

# common bits for SQLite users in our codebase
package PublicInbox::SQLiteUtil;
use v5.12;
use autodie qw(open truncate);
use DBI ();

my %SQLITE_GLOB_MAP = (
	'[' => '[[]',
	']' => '[]]',
	'*' => '[*]',
	'?' => '[?]'
);

# n.b. GLOB doesn't seem to work on data inserted w/ SQL_BLOB
sub escape_glob ($) {
	my ($s) = @_;
	$s =~ s/([\[\]\*\?])/$SQLITE_GLOB_MAP{$1}/sge;
	$s;
}

# DBD::SQLite maps REGEXP to use perlre, and that works on SQL_BLOB
# whereas GLOB and LIKE don't seem to...
sub mk_sqlite_re ($$) {
	my ($pfx, $anywhere) = @_;
	ref($pfx) ? $pfx # assume qr// Regexp
		: ($anywhere ? '.*' : '^')."\Q$pfx\E.*";
}

sub create_db ($;$) {
	my ($f, $opt) = @_;
	my ($dir) = ($f =~ m!(.+)/[^/]+\z!);
	unless ($opt->{cow}) {
		require PublicInbox::Syscall;
		PublicInbox::Syscall::nodatacow_dir($dir); # for journal/shm/wal
	}
	# SQLite defaults mode to 0644, we want 0666 to respect umask
	open my $fh, '+>>', $f;
}

sub dbh_open ($;@) {
	my ($f, @opt) = @_;
	DBI->connect("dbi:SQLite:dbname=$f",'','', {
		AutoCommit => 1,
		RaiseError => 1,
		PrintError => 0,
		sqlite_use_immediate_transaction => 1,
		@opt,
	});
}

# try to save some space on SQLite 3.27+ using `VACUUM INTO',
# preserves WAL
sub copy_db ($$;$) {
	my ($dbh, $f, $opt) = @_;
	if (-e $f) { # VACUUM INTO requires empty/non-existent file
		truncate($f, 0) if -s _;
	} else {
		create_db $f, $opt;
	}
	if (eval('"v$DBD::SQLite::sqlite_version"') ge v3.27) {
		$dbh->do('VACUUM INTO '.$dbh->quote($f));
		if ($dbh->selectrow_array('PRAGMA journal_mode') eq 'wal') {
			dbh_open($f)->do('PRAGMA journal_mode = WAL');
		}
	} else {
		$dbh->sqlite_backup_to_file($f);
	}
}

1;
