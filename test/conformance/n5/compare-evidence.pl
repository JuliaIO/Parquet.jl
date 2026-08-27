#!/usr/bin/env perl
use strict;
use warnings;
use JSON::PP;

sub fail {
    die "compare-evidence.pl: $_[0]\n";
}

sub usage {
    die "usage: compare-evidence.pl --manifest FILE --unsupported FILE "
        . "--java FILE --rust FILE --output FILE\n";
}

sub arguments {
    my %values;
    while (@ARGV) {
        my $key = shift @ARGV;
        usage() unless $key =~ /^--(?:manifest|unsupported|java|rust|output)$/;
        usage() unless @ARGV;
        fail("duplicate option $key") if exists $values{$key};
        $values{$key} = shift @ARGV;
    }
    for my $key (qw(--manifest --unsupported --java --rust --output)) {
        usage() unless exists $values{$key};
    }
    return %values;
}

sub lines {
    my ($path) = @_;
    open my $input, '<:raw', $path or fail("cannot open $path: $!");
    my @lines = <$input>;
    close $input or fail("cannot close $path: $!");
    chomp @lines;
    s/\r$// for @lines;
    return @lines;
}

sub normalized_path {
    my ($path, $label) = @_;
    fail("$label is empty") unless length $path;
    fail("$label is absolute") if $path =~ m{^/};
    fail("$label contains a backslash") if $path =~ /\\/;
    fail("$label is not normalized") if grep { $_ eq '' || $_ eq '.' || $_ eq '..' }
        split m{/}, $path, -1;
    return $path;
}

sub parse_manifest {
    my ($path) = @_;
    my @input = lines($path);
    fail("fixture manifest is empty") unless @input;
    my $header = shift @input;
    my $expected = join "\t", qw(kind case_id page_version codec reference
        target reference_sha256 target_sha256);
    fail("fixture manifest header differs") unless $header eq $expected;
    my @rows;
    my %targets;
    for my $index (0 .. $#input) {
        my @fields = split /\t/, $input[$index], -1;
        fail("fixture manifest row @{[$index + 2]} has wrong field count")
            unless @fields == 8;
        my ($kind, $case, $page, $codec, $reference, $target,
            $reference_hash, $target_hash) = @fields;
        fail("unsupported fixture kind $kind")
            unless $kind =~ /^(?:binding|provenance|property|external)$/;
        fail("empty case ID") unless length $case;
        fail("unsupported page version $page") unless $page =~ /^v[12]$/;
        fail("unsupported codec $codec") unless $codec =~
            /^(?:uncompressed|snappy|gzip|brotli|zstd|lz4_raw)$/;
        normalized_path($reference, "reference path");
        normalized_path($target, "target path");
        fail("invalid reference SHA-256") unless $reference_hash =~ /^[0-9a-f]{64}$/;
        fail("invalid target SHA-256") unless $target_hash =~ /^[0-9a-f]{64}$/;
        fail("duplicate target $target") if $targets{$target}++;
        push @rows, {
            kind => $kind,
            case_id => $case,
            page_version => $page,
            codec => $codec,
            reference => $reference,
            target => $target,
            reference_sha256 => $reference_hash,
            target_sha256 => $target_hash,
        };
    }
    fail("fixture manifest must contain 256 mappings") unless @rows == 256;
    return @rows;
}

sub parse_unsupported {
    my ($path) = @_;
    my @input = lines($path);
    fail("unsupported allowlist is empty") unless @input;
    fail("unsupported allowlist header differs")
        unless shift(@input) eq join("\t", qw(oracle file error_class error_message));
    my %expected;
    for my $index (0 .. $#input) {
        my @fields = split /\t/, $input[$index], -1;
        fail("unsupported row @{[$index + 2]} has wrong field count")
            unless @fields == 4;
        my ($oracle, $file, $class, $message) = @fields;
        fail("unknown unsupported oracle $oracle")
            unless $oracle eq 'parquet-java' || $oracle eq 'arrow-rs';
        normalized_path($file, "unsupported file");
        fail("empty unsupported class") unless length $class;
        fail("empty unsupported message") unless length $message;
        my $key = "$oracle\0$file";
        fail("duplicate unsupported key $oracle/$file") if exists $expected{$key};
        $expected{$key} = [$class, $message];
    }
    return %expected;
}

sub parse_json_value {
    my ($text, $label) = @_;
    my $value = eval { JSON::PP->new->utf8->decode($text) };
    fail("cannot decode $label: $@") if $@;
    return $value;
}

sub exact_keys {
    my ($value, $required, $label) = @_;
    fail("$label is not an object") unless ref($value) eq 'HASH';
    my %expected = map { $_ => 1 } @$required;
    my @actual = sort keys %$value;
    my @wanted = sort keys %expected;
    fail("$label keys differ") unless join("\0", @actual) eq join("\0", @wanted);
}

sub require_object {
    my ($value, $label) = @_;
    fail("$label is not an object") unless ref($value) eq 'HASH';
    return $value;
}

sub require_array {
    my ($value, $label) = @_;
    fail("$label is not an array") unless ref($value) eq 'ARRAY';
    return $value;
}

sub require_string {
    my ($value, $label) = @_;
    fail("$label is not a string") unless defined($value) && !ref($value)
        && JSON::PP->new->allow_nonref->encode($value) =~ /^"/;
    return $value;
}

sub require_integer {
    my ($value, $label) = @_;
    fail("$label is not an integer") unless defined($value) && !ref($value)
        && JSON::PP->new->allow_nonref->encode($value) =~
            /^-?(?:0|[1-9][0-9]*)$/;
    return 0 + $value;
}

sub require_nonnegative_integer {
    my ($value, $label) = @_;
    my $integer = require_integer($value, $label);
    fail("$label is negative") if $integer < 0;
    return $integer;
}

sub require_false {
    my ($value, $label) = @_;
    fail("$label is not false") unless JSON::PP::is_bool($value) && !$value;
}

sub validate_java_avro {
    my ($value, $label) = @_;
    exact_keys($value, [qw(status read_schema materialized_schema rows
        normalized_rows error_class error_message error_stack exception_chain)], $label);
    require_string($value->{status}, "$label status");
    require_array($value->{rows}, "$label rows");
    require_array($value->{normalized_rows}, "$label normalized rows");
    require_array($value->{error_stack}, "$label error stack");
    require_array($value->{exception_chain}, "$label exception chain");
}

sub validate_java_file_record {
    my ($record, $label) = @_;
    exact_keys($record, [qw(record file sha256 case_id page_version created_by
        row_count metadata physical_schema row_groups raw_group_rows columns avro)], $label);
    require_string($record->{file}, "$label file");
    fail("$label SHA-256 is invalid") unless
        require_string($record->{sha256}, "$label SHA-256") =~ /^[0-9a-f]{64}$/;
    require_nonnegative_integer($record->{row_count}, "$label row count");
    require_object($record->{metadata}, "$label metadata");
    require_string($record->{physical_schema}, "$label physical schema");
    my $row_groups = require_array($record->{row_groups}, "$label row groups");
    require_array($record->{raw_group_rows}, "$label raw rows");
    my $columns = require_array($record->{columns}, "$label columns");
    for my $index (0 .. $#$row_groups) {
        my $group = $row_groups->[$index];
        exact_keys($group, [qw(ordinal row_count total_byte_size columns)],
            "$label row group $index");
        require_nonnegative_integer($group->{ordinal}, "$label row group ordinal");
        require_nonnegative_integer($group->{row_count}, "$label row group rows");
        require_nonnegative_integer($group->{total_byte_size}, "$label row group bytes");
        my $chunks = require_array($group->{columns}, "$label row group columns");
        for my $chunk_index (0 .. $#$chunks) {
            my $chunk = $chunks->[$chunk_index];
            exact_keys($chunk, [qw(path value_count codec encodings
                total_compressed_size total_uncompressed_size)],
                "$label row group $index chunk $chunk_index");
            require_string($chunk->{path}, "$label chunk path");
            require_string($chunk->{codec}, "$label chunk codec");
            require_array($chunk->{encodings}, "$label chunk encodings");
            require_nonnegative_integer($chunk->{value_count}, "$label chunk values");
            require_nonnegative_integer($chunk->{total_compressed_size},
                "$label chunk compressed bytes");
            require_nonnegative_integer($chunk->{total_uncompressed_size},
                "$label chunk uncompressed bytes");
        }
    }
    for my $index (0 .. $#$columns) {
        my $column = $columns->[$index];
        exact_keys($column, [qw(path physical_type logical_type type_length
            max_repetition_level max_definition_level repetition definition dense
            dictionaries pages row_groups)], "$label column $index");
        require_string($column->{path}, "$label column path");
        require_string($column->{physical_type}, "$label column physical type");
        require_integer($column->{type_length}, "$label column type length");
        require_nonnegative_integer($column->{max_repetition_level},
            "$label column maximum repetition level");
        require_nonnegative_integer($column->{max_definition_level},
            "$label column maximum definition level");
        require_array($column->{repetition}, "$label column repetitions");
        require_array($column->{definition}, "$label column definitions");
        require_array($column->{dense}, "$label column dense values");
        require_array($column->{dictionaries}, "$label column dictionaries");
        my $pages = require_array($column->{pages}, "$label column pages");
        for my $page_index (0 .. $#$pages) {
            my $page = $pages->[$page_index];
            exact_keys($page, [qw(row_group ordinal type encoding value_count
                row_count null_count compressed_size uncompressed_size
                index_row_count)], "$label column $index page $page_index");
            require_string($page->{type}, "$label page type");
            require_string($page->{encoding}, "$label page encoding");
            for my $field (qw(row_group ordinal value_count row_count null_count
                    compressed_size uncompressed_size index_row_count)) {
                require_nonnegative_integer($page->{$field}, "$label page $field")
                    if defined $page->{$field};
            }
        }
        require_array($column->{row_groups}, "$label column row groups");
    }
    my $avro = require_object($record->{avro}, "$label Avro");
    exact_keys($avro, [qw(add_list_element_records inferred explicit)], "$label Avro");
    require_false($avro->{add_list_element_records}, "$label Avro list setting");
    validate_java_avro($avro->{inferred}, "$label inferred Avro");
    validate_java_avro($avro->{explicit}, "$label explicit Avro")
        if defined $avro->{explicit};
}

sub validate_rust_file_evidence {
    my ($value, $label) = @_;
    exact_keys($value, [qw(case_id file_name sha256 file_bytes rows row_groups
        physical_schema columns arrow)], $label);
    require_string($value->{case_id}, "$label case ID");
    require_string($value->{file_name}, "$label file name");
    fail("$label SHA-256 is invalid") unless
        require_string($value->{sha256}, "$label SHA-256") =~ /^[0-9a-f]{64}$/;
    require_nonnegative_integer($value->{file_bytes}, "$label file bytes");
    require_nonnegative_integer($value->{rows}, "$label rows");
    require_nonnegative_integer($value->{row_groups}, "$label row-group count");
    require_string($value->{physical_schema}, "$label physical schema");
    my $columns = require_array($value->{columns}, "$label columns");
    for my $index (0 .. $#$columns) {
        my $column = $columns->[$index];
        exact_keys($column, [qw(path physical_type maximum_definition_level
            maximum_repetition_level row_groups)], "$label column $index");
        require_array($column->{path}, "$label column path");
        require_string($column->{physical_type}, "$label column physical type");
        require_nonnegative_integer($column->{maximum_definition_level},
            "$label column maximum definition level");
        require_nonnegative_integer($column->{maximum_repetition_level},
            "$label column maximum repetition level");
        my $groups = require_array($column->{row_groups}, "$label column row groups");
        for my $group_index (0 .. $#$groups) {
            my $group = $groups->[$group_index];
            exact_keys($group, [qw(row_group rows compression repetition definition
                dense_values pages)], "$label column $index row group $group_index");
            require_nonnegative_integer($group->{row_group}, "$label row-group ordinal");
            require_nonnegative_integer($group->{rows}, "$label row-group rows");
            require_string($group->{compression}, "$label row-group compression");
            require_array($group->{repetition}, "$label row-group repetitions");
            require_array($group->{definition}, "$label row-group definitions");
            require_array($group->{dense_values}, "$label row-group dense values");
            my $pages = require_array($group->{pages}, "$label row-group pages");
            for my $page_index (0 .. $#$pages) {
                my $page = $pages->[$page_index];
                exact_keys($page, [qw(kind version encoding values rows)],
                    "$label page $page_index");
                require_string($page->{kind}, "$label page kind");
                require_string($page->{encoding}, "$label page encoding");
                require_nonnegative_integer($page->{values}, "$label page values");
                require_string($page->{version}, "$label page version")
                    if defined $page->{version};
                require_nonnegative_integer($page->{rows}, "$label page rows")
                    if defined $page->{rows};
            }
        }
    }
    my $arrow = require_object($value->{arrow}, "$label Arrow");
    exact_keys($arrow, [qw(status schema canonical_rows json_rows ordered_map_rows
        diagnostic)], "$label Arrow");
    require_string($arrow->{status}, "$label Arrow status");
    require_array($arrow->{canonical_rows}, "$label Arrow canonical rows");
    require_array($arrow->{json_rows}, "$label Arrow JSON rows");
}

sub parse_java {
    my ($path, $unsupported) = @_;
    my @input = lines($path);
    fail("Java evidence is empty") unless @input;
    my $run = parse_json_value(shift(@input), "Java run record");
    exact_keys($run, [qw(record schema_version oracle command parquet_java_version
        parquet_java_commit avro_add_list_element_records fixture_writer java_version
        java_vendor file_count supported_count unsupported_count)], "Java run record");
    my $writer = require_object($run->{fixture_writer}, "Java fixture writer");
    exact_keys($writer, [qw(compression dictionary page_size row_group_size
        page_checksums validation page_versions)], "Java fixture writer");
    require_false($run->{avro_add_list_element_records},
        "Java Avro list-element setting");
    require_false($writer->{dictionary}, "Java fixture dictionary setting");
    fail("Java fixture page-checksum setting differs") unless
        JSON::PP::is_bool($writer->{page_checksums}) && $writer->{page_checksums};
    fail("Java fixture validation setting differs") unless
        JSON::PP::is_bool($writer->{validation}) && $writer->{validation};
    require_nonnegative_integer($run->{file_count}, "Java run file count");
    require_nonnegative_integer($run->{supported_count}, "Java supported count");
    require_nonnegative_integer($run->{unsupported_count}, "Java unsupported count");
    fail("Java run record differs") unless ($run->{record} // '') eq 'run'
        && ($run->{schema_version} // 0) == 1
        && ($run->{oracle} // '') eq 'parquet-java'
        && ($run->{command} // '') eq 'audit'
        && ($run->{parquet_java_version} // '') eq '1.17.1'
        && ($run->{parquet_java_commit} // '') eq
            '78a8d3230eb4769db93de5f2f2e18363c04cae81'
        && ($run->{java_version} // '') eq '11.0.28'
        && ($run->{java_vendor} // '') eq 'Eclipse Adoptium'
        && ($writer->{compression} // '') eq 'UNCOMPRESSED'
        && ($writer->{page_size} // 0) == 1_048_576
        && ($writer->{row_group_size} // 0) == 134_217_728;
    same_json($writer->{page_versions}, ['v1', 'v2'],
        "Java fixture page versions");
    my %files;
    my @order;
    my $supported = 0;
    my $unsupported_count = 0;
    for my $index (0 .. $#input) {
        my $record = parse_json_value($input[$index], "Java record @{[$index + 2]}");
        fail("Java record is not an object") unless ref($record) eq 'HASH';
        my $kind = $record->{record} // '';
        fail("unknown Java record kind $kind")
            unless $kind eq 'file' || $kind eq 'unsupported';
        if ($kind eq 'file') {
            validate_java_file_record($record, "Java record @{[$index + 2]}");
        } else {
            exact_keys($record, [qw(record file error_class error_message)],
                "Java record @{[$index + 2]}");
        }
        my $file = normalized_path($record->{file} // '', "Java evidence file");
        fail("duplicate Java evidence file $file") if exists $files{$file};
        push @order, $file;
        if ($kind eq 'unsupported') {
            my $key = "parquet-java\0$file";
            fail("unexpected Java unsupported file $file") unless exists $unsupported->{$key};
            my ($class, $message) = @{$unsupported->{$key}};
            my $actual_message = defined $record->{error_message}
                ? $record->{error_message} : '<null>';
            fail("Java unsupported class differs for $file")
                unless ($record->{error_class} // '') eq $class;
            fail("Java unsupported message differs for $file")
                unless $actual_message eq $message;
            $unsupported_count++;
        } else {
            fail("allowlisted Java file became supported: $file")
                if exists $unsupported->{"parquet-java\0$file"};
            $supported++;
        }
        $files{$file} = $record;
    }
    my @sorted = sort @order;
    fail("Java evidence order is not deterministic")
        unless join("\0", @order) eq join("\0", @sorted);
    fail("Java run file count differs") unless ($run->{file_count} // -1) == @input;
    fail("Java run supported count differs")
        unless ($run->{supported_count} // -1) == $supported;
    fail("Java run unsupported count differs")
        unless ($run->{unsupported_count} // -1) == $unsupported_count;
    return (\%files, $supported, $unsupported_count);
}

sub parse_rust {
    my ($path, $unsupported) = @_;
    my $text = join("\n", lines($path)) . "\n";
    my $report = parse_json_value($text, "Rust evidence");
    exact_keys($report, [qw(evidence_version oracle action file_count supported_count
        unsupported_count files)], "Rust evidence");
    my $oracle = $report->{oracle};
    exact_keys($oracle, [qw(name version commit rust_toolchain rustc cargo)],
        "Rust oracle record");
    require_nonnegative_integer($report->{file_count}, "Rust run file count");
    require_nonnegative_integer($report->{supported_count}, "Rust supported count");
    require_nonnegative_integer($report->{unsupported_count}, "Rust unsupported count");
    fail("Rust oracle record differs") unless
        ($report->{evidence_version} // 0) == 1
        && ($report->{action} // '') eq 'audit'
        && ($oracle->{name} // '') eq 'arrow-rs'
        && ($oracle->{version} // '') eq '59.2.0'
        && ($oracle->{commit} // '') eq
            '782e5a685501a9db6cc8e9a3b7cbff894940c47a'
        && ($oracle->{rust_toolchain} // '') eq '1.96.1';
    my $records = $report->{files};
    fail("Rust files are not an array") unless ref($records) eq 'ARRAY';
    my %files;
    my @order;
    my $supported = 0;
    my $unsupported_count = 0;
    for my $record (@$records) {
        exact_keys($record, [qw(status file evidence error)], "Rust file record");
        my $file = normalized_path($record->{file} // '', "Rust evidence file");
        fail("duplicate Rust evidence file $file") if exists $files{$file};
        push @order, $file;
        my $status = $record->{status} // '';
        fail("unknown Rust status for $file")
            unless $status eq 'supported' || $status eq 'unsupported';
        if ($status eq 'unsupported') {
            my $key = "arrow-rs\0$file";
            fail("unexpected Rust unsupported file $file") unless exists $unsupported->{$key};
            my ($class, $message) = @{$unsupported->{$key}};
            fail("Rust unsupported class differs for $file") unless $class eq 'error';
            fail("Rust unsupported message differs for $file")
                unless ($record->{error} // '') eq $message;
            fail("Rust unsupported evidence is not empty")
                if defined $record->{evidence};
            $unsupported_count++;
        } else {
            fail("allowlisted Rust file became supported: $file")
                if exists $unsupported->{"arrow-rs\0$file"};
            fail("Rust supported evidence is absent")
                unless ref($record->{evidence}) eq 'HASH';
            fail("Rust supported error is not null") if defined $record->{error};
            validate_rust_file_evidence($record->{evidence},
                "Rust evidence for $file");
            fail("Rust evidence case ID differs for $file")
                unless $record->{evidence}->{case_id} eq $file;
            my @parts = split m{/}, $file;
            fail("Rust evidence file name differs for $file")
                unless $record->{evidence}->{file_name} eq $parts[-1];
            $supported++;
        }
        $files{$file} = $record;
    }
    my @sorted = sort @order;
    fail("Rust evidence order is not deterministic")
        unless join("\0", @order) eq join("\0", @sorted);
    fail("Rust run file count differs")
        unless ($report->{file_count} // -1) == @$records;
    fail("Rust run supported count differs")
        unless ($report->{supported_count} // -1) == $supported;
    fail("Rust run unsupported count differs")
        unless ($report->{unsupported_count} // -1) == $unsupported_count;
    return (\%files, $supported, $unsupported_count);
}

my $canonical = JSON::PP->new->canonical->allow_nonref;

sub expected_codec {
    my ($codec) = @_;
    my %values = (
        uncompressed => 'UNCOMPRESSED',
        snappy => 'SNAPPY',
        gzip => 'GZIP',
        brotli => 'BROTLI',
        zstd => 'ZSTD',
        lz4_raw => 'LZ4_RAW',
    );
    fail("unsupported expected codec $codec") unless exists $values{$codec};
    return $values{$codec};
}

sub same_json {
    my ($left, $right, $label) = @_;
    fail("$label differs") unless $canonical->encode($left) eq
        $canonical->encode($right);
}

sub java_file {
    my ($record, $expected_hash, $label) = @_;
    fail("$label is not supported") unless ($record->{record} // '') eq 'file';
    fail("$label SHA-256 differs")
        unless ($record->{sha256} // '') eq $expected_hash;
    return $record;
}

sub compare_java {
    my ($source, $target, $mapping) = @_;
    for my $side (['reference', $source], ['target', $target]) {
        my ($name, $record) = @$side;
        fail("Java $name case ID differs for $mapping->{target}")
            if defined($record->{case_id}) &&
                $record->{case_id} ne $mapping->{case_id};
        fail("Java $name page version differs for $mapping->{target}")
            if defined($record->{page_version}) &&
                $record->{page_version} ne $mapping->{page_version};
    }
    same_json($source->{physical_schema}, $target->{physical_schema},
        "Java physical schema for $mapping->{target}");
    same_json($source->{row_count}, $target->{row_count},
        "Java row count for $mapping->{target}");
    same_json($source->{raw_group_rows}, $target->{raw_group_rows},
        "Java raw rows for $mapping->{target}");
    my $source_columns = $source->{columns};
    my $target_columns = $target->{columns};
    fail("Java columns are not arrays") unless ref($source_columns) eq 'ARRAY'
        && ref($target_columns) eq 'ARRAY';
    fail("Java column count differs for $mapping->{target}")
        unless @$source_columns == @$target_columns;
    for my $index (0 .. $#$source_columns) {
        my $left = $source_columns->[$index];
        my $right = $target_columns->[$index];
        for my $field (qw(path physical_type logical_type type_length
                max_repetition_level max_definition_level repetition definition dense)) {
            same_json($left->{$field}, $right->{$field},
                "Java column $index $field for $mapping->{target}");
        }
        fail("Java target dictionary is present for $mapping->{target}")
            if @{$right->{dictionaries} // []};
        fail("Java reference dictionary is present for $mapping->{target}")
            if @{$left->{dictionaries} // []};
        my $source_pages = $left->{pages};
        fail("Java reference pages are empty for $mapping->{target}")
            unless ref($source_pages) eq 'ARRAY' && @$source_pages;
        my $pages = $right->{pages};
        fail("Java target pages are empty for $mapping->{target}")
            unless ref($pages) eq 'ARRAY' && @$pages;
        my $expected_type = $mapping->{page_version} eq 'v1'
            ? 'DATA_PAGE_V1' : 'DATA_PAGE_V2';
        for my $page (@$source_pages) {
            fail("Java reference page type differs for $mapping->{target}")
                unless ($page->{type} // '') eq $expected_type;
        }
        my ($values, $rows) = (0, 0);
        for my $page (@$pages) {
            fail("Java target page type differs for $mapping->{target}")
                unless ($page->{type} // '') eq $expected_type;
            fail("Java target page encoding differs for $mapping->{target}")
                unless ($page->{encoding} // '') eq 'PLAIN';
            $values += $page->{value_count};
            $rows += $page->{row_count};
        }
        fail("Java target page values differ for $mapping->{target}")
            unless $values == @{$right->{repetition}};
        my $derived_rows = grep { $_ == 0 } @{$right->{repetition}};
        fail("Java target page rows differ for $mapping->{target}")
            unless $rows == $derived_rows;
    }
    my $row_groups = require_array($target->{row_groups},
        "Java target row groups for $mapping->{target}");
    fail("Java target row groups are empty for $mapping->{target}")
        unless @$row_groups;
    for my $group (@$row_groups) {
        my $chunks = require_array($group->{columns},
            "Java target row-group columns for $mapping->{target}");
        fail("Java target row-group columns are empty for $mapping->{target}")
            unless @$chunks;
        for my $column (@$chunks) {
            fail("Java target codec differs for $mapping->{target}")
                unless ($column->{codec} // '') eq expected_codec($mapping->{codec});
        }
    }
    my $source_groups = require_array($source->{row_groups},
        "Java reference row groups for $mapping->{target}");
    fail("Java reference row groups are empty for $mapping->{target}")
        unless @$source_groups;
    for my $group (@$source_groups) {
        my $chunks = require_array($group->{columns},
            "Java reference row-group columns for $mapping->{target}");
        fail("Java reference row-group columns are empty for $mapping->{target}")
            unless @$chunks;
        for my $column (@$chunks) {
            fail("Java reference codec differs for $mapping->{target}")
                unless ($column->{codec} // '') eq 'UNCOMPRESSED';
        }
    }
    my $source_avro = $source->{avro}->{inferred};
    my $target_avro = $target->{avro}->{inferred};
    if (($source_avro->{status} // '') eq 'success') {
        fail("Java Avro target rejected $mapping->{target}")
            unless ($target_avro->{status} // '') eq 'success';
        same_json($source_avro->{materialized_schema},
            $target_avro->{materialized_schema},
            "Java Avro schema for $mapping->{target}");
        same_json($source_avro->{normalized_rows},
            $target_avro->{normalized_rows},
            "Java Avro rows for $mapping->{target}");
    }
}

sub rust_file {
    my ($record, $expected_hash, $label) = @_;
    fail("$label is not supported") unless ($record->{status} // '') eq 'supported';
    my $evidence = $record->{evidence};
    fail("$label SHA-256 differs")
        unless ($evidence->{sha256} // '') eq $expected_hash;
    return $evidence;
}

sub flattened {
    my ($column, $field) = @_;
    my @values;
    for my $group (@{$column->{row_groups} // []}) {
        push @values, @{$group->{$field} // []};
    }
    return \@values;
}

sub compare_rust {
    my ($source, $target, $mapping) = @_;
    same_json($source->{physical_schema}, $target->{physical_schema},
        "Rust physical schema for $mapping->{target}");
    same_json($source->{rows}, $target->{rows},
        "Rust row count for $mapping->{target}");
    my $source_columns = $source->{columns};
    my $target_columns = $target->{columns};
    fail("Rust columns are not arrays") unless ref($source_columns) eq 'ARRAY'
        && ref($target_columns) eq 'ARRAY';
    fail("Rust column count differs for $mapping->{target}")
        unless @$source_columns == @$target_columns;
    for my $index (0 .. $#$source_columns) {
        my $left = $source_columns->[$index];
        my $right = $target_columns->[$index];
        for my $field (qw(path physical_type maximum_definition_level
                maximum_repetition_level)) {
            same_json($left->{$field}, $right->{$field},
                "Rust column $index $field for $mapping->{target}");
        }
        for my $field (qw(repetition definition dense_values)) {
            same_json(flattened($left, $field), flattened($right, $field),
                "Rust column $index $field for $mapping->{target}");
        }
        my $source_groups = require_array($left->{row_groups},
            "Rust reference row groups for $mapping->{target}");
        fail("Rust reference row groups are empty for $mapping->{target}")
            unless @$source_groups;
        for my $group (@$source_groups) {
            fail("Rust reference codec differs for $mapping->{target}")
                unless ($group->{compression} // '') eq 'UNCOMPRESSED';
            my $source_pages = require_array($group->{pages},
                "Rust reference pages for $mapping->{target}");
            fail("Rust reference pages are empty for $mapping->{target}")
                unless @$source_pages;
            for my $page (@$source_pages) {
                fail("Rust reference dictionary is present for $mapping->{target}")
                    unless ($page->{kind} // '') eq 'data';
                fail("Rust reference page version differs for $mapping->{target}")
                    unless ($page->{version} // '') eq $mapping->{page_version};
            }
        }
        my $groups = require_array($right->{row_groups},
            "Rust target row groups for $mapping->{target}");
        fail("Rust target row groups are empty for $mapping->{target}")
            unless @$groups;
        for my $group (@$groups) {
            fail("Rust target codec differs for $mapping->{target}")
                unless ($group->{compression} // '') eq
                    expected_codec($mapping->{codec});
        }
        my @pages = map { @{$_->{pages} // []} } @{$right->{row_groups} // []};
        fail("Rust target pages are empty for $mapping->{target}") unless @pages;
        my ($values, $rows) = (0, 0);
        for my $page (@pages) {
            fail("Rust target dictionary is present for $mapping->{target}")
                unless ($page->{kind} // '') eq 'data';
            fail("Rust target page version differs for $mapping->{target}")
                unless ($page->{version} // '') eq $mapping->{page_version};
            fail("Rust target page encoding differs for $mapping->{target}")
                unless ($page->{encoding} // '') eq 'PLAIN';
            $values += $page->{values};
            $rows += $page->{rows};
        }
        my $repetition = flattened($right, 'repetition');
        fail("Rust target page values differ for $mapping->{target}")
            unless $values == @$repetition;
        my $derived_rows = grep { $_ == 0 } @$repetition;
        fail("Rust target page rows differ for $mapping->{target}")
            unless $rows == $derived_rows;
    }
    my $source_arrow = $source->{arrow};
    my $target_arrow = $target->{arrow};
    if (($source_arrow->{status} // '') eq 'success') {
        fail("Rust Arrow target rejected $mapping->{target}")
            unless ($target_arrow->{status} // '') eq 'success';
        for my $field (qw(schema canonical_rows ordered_map_rows)) {
            same_json($source_arrow->{$field}, $target_arrow->{$field},
                "Rust Arrow $field for $mapping->{target}");
        }
    }
}

my %args = arguments();
my @mappings = parse_manifest($args{'--manifest'});
my %unsupported = parse_unsupported($args{'--unsupported'});
my ($java, $java_supported, $java_unsupported) =
    parse_java($args{'--java'}, \%unsupported);
my ($rust, $rust_supported, $rust_unsupported) =
    parse_rust($args{'--rust'}, \%unsupported);

my %expected_files;
for my $mapping (@mappings) {
    $expected_files{$mapping->{reference}} = 1;
    $expected_files{$mapping->{target}} = 1;
}
fail("expected 352 unique Parquet inputs") unless keys(%expected_files) == 352;
for my $oracle (['Java', $java], ['Rust', $rust]) {
    my ($label, $files) = @$oracle;
    my @actual = sort keys %$files;
    my @expected = sort keys %expected_files;
    fail("$label input file set differs")
        unless join("\0", @actual) eq join("\0", @expected);
}
for my $key (keys %unsupported) {
    my ($oracle, $file) = split /\0/, $key, 2;
    my $files = $oracle eq 'parquet-java' ? $java : $rust;
    fail("unused unsupported allowlist entry $oracle/$file")
        unless exists $files->{$file};
}

my ($java_pairs, $rust_pairs, $externally_supported, $paired_mappings) =
    (0, 0, 0, 0);
my %kind_counts;
my %codec_counts;
for my $mapping (@mappings) {
    $kind_counts{$mapping->{kind}}++;
    $codec_counts{$mapping->{codec}}++ if $mapping->{kind} eq 'property';
    my $java_source = $java->{$mapping->{reference}};
    my $java_target = $java->{$mapping->{target}};
    my $rust_source = $rust->{$mapping->{reference}};
    my $rust_target = $rust->{$mapping->{target}};
    my $java_ok = ($java_target->{record} // '') eq 'file';
    my $rust_ok = ($rust_target->{status} // '') eq 'supported';
    my $paired = 0;
    $externally_supported++ if $java_ok || $rust_ok;
    if ($java_ok && ($java_source->{record} // '') eq 'file') {
        compare_java(
            java_file($java_source, $mapping->{reference_sha256}, 'Java reference'),
            java_file($java_target, $mapping->{target_sha256}, 'Java target'),
            $mapping);
        $java_pairs++;
        $paired = 1;
    }
    if ($rust_ok && ($rust_source->{status} // '') eq 'supported') {
        compare_rust(
            rust_file($rust_source, $mapping->{reference_sha256}, 'Rust reference'),
            rust_file($rust_target, $mapping->{target_sha256}, 'Rust target'),
            $mapping);
        $rust_pairs++;
        $paired = 1;
    }
    if ($mapping->{kind} ne 'provenance') {
        fail("mapping has no successful paired external comparison: "
            . "$mapping->{reference} -> $mapping->{target}") unless $paired;
        $paired_mappings++;
    }
}
my %expected_kind_counts = (
    binding => 24,
    provenance => 2,
    property => 192,
    external => 38,
);
fail("fixture kind set differs") unless scalar(keys %kind_counts) ==
    scalar(keys %expected_kind_counts);
for my $kind (sort keys %expected_kind_counts) {
    fail("fixture kind count differs for $kind") unless
        ($kind_counts{$kind} // 0) == $expected_kind_counts{$kind};
}
fail("property codec matrix differs") unless scalar(keys %codec_counts) == 6
    && !grep { $codec_counts{$_} != 32 }
        qw(uncompressed snappy gzip brotli zstd lz4_raw);

my $summary = {
    schema_version => 1,
    status => 'ok',
    mappings => scalar(@mappings),
    input_files => scalar(keys %expected_files),
    kinds => \%kind_counts,
    property_codecs => \%codec_counts,
    java => {
        supported_files => $java_supported,
        unsupported_files => $java_unsupported,
        compared_mappings => $java_pairs,
    },
    rust => {
        supported_files => $rust_supported,
        unsupported_files => $rust_unsupported,
        compared_mappings => $rust_pairs,
    },
    mappings_with_external_success => $externally_supported,
    paired_mappings => $paired_mappings,
};
open my $output, '>:raw', $args{'--output'}
    or fail("cannot open $args{'--output'}: $!");
print {$output} JSON::PP->new->canonical->pretty->encode($summary)
    or fail("cannot write $args{'--output'}: $!");
close $output or fail("cannot close $args{'--output'}: $!");
print "validated 256 N5 Julia mappings with Java and Rust\n";
