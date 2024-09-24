package ARFF

import "core:bufio"
import "core:fmt"
import "core:os"
import "core:strings"

ARFFData :: struct {
	relation:   string "@RELATION",
	attributes: [dynamic]Attribute "@ATTRIBUTE",
	data:       [dynamic][]string "@DATA",
}

Attribute :: union {
	NumericAttribute,
	IntegerAttribute,
	RealAttribute,
	NominalAttribute,
	StringAttribute,
	DateAttribute,
}

NumericAttribute :: struct {
	name: string,
	type: typeid,
}

IntegerAttribute :: struct {
	name: string,
	type: typeid,
}

RealAttribute :: struct {
	name: string,
	type: typeid,
}

NominalAttribute :: struct {
	name:   string,
	type:   typeid,
	values: map[string]i64,
}

StringAttribute :: struct {
	name: string,
	type: typeid,
}

DateAttribute :: struct {
	name:   string,
	type:   typeid,
	format: string,
}

all_reqs :: "https://waikato.github.io/weka-wiki/formats_and_processing/arff_stable/#overview"
relation_reqs :: "https://waikato.github.io/weka-wiki/formats_and_processing/arff_stable/#the-relation-declaration"
attribute_reqs :: "https://waikato.github.io/weka-wiki/formats_and_processing/arff_stable/#the-attribute-declarations"
data_reqs :: "https://waikato.github.io/weka-wiki/formats_and_processing/arff_stable/#the-data-declaration"
instance_reqs :: "https://waikato.github.io/weka-wiki/formats_and_processing/arff_stable/#the-instance-data"

arff_split :: proc(entry: string, allocator := context.allocator) -> (fields: [dynamic]string) {
	Mode :: enum {
		normal,
		space,
		ticks,
		quotes,
	}
	using Mode
	mode: Mode = space
	start := 0
	length := len(entry) - 1

	for ch, idx in entry {
		switch mode {
		case normal:
			if ch == '\'' {mode = ticks} else if ch == '\"' {mode = quotes} else {
				if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
					mode = space
					append(&fields, strings.clone(entry[start:idx]))
				} else if idx == length {
					append(&fields, strings.clone(entry[start:idx + 1]))
				}
			}
		case space:
			if ch >= '!' || ch <= '~' {
				start = idx
				if ch == '\'' {
					mode = ticks
				} else if ch == '\"' {
					mode = quotes
				} else {
					mode = normal
				}
			}
		case ticks:
			if ch == '\'' {mode = normal}
		case quotes:
			if ch == '\"' {mode = normal}
		}
	}
	return fields
}

create_validate_attribute :: proc(
	fields: []string,
	allocator := context.allocator,
) -> (
	Attribute,
	bool,
) {
	length := len(fields)
	valid := (length == 3 || length == 4)

	if valid {
		name := strings.clone(fields[1])
		switch fields[2] {
		case "NUMERIC", "numeric":
			return NumericAttribute{name, f64}, valid
		case "INTEGER", "integer":
			return IntegerAttribute{name, i64}, valid
		case "REAL", "real":
			return RealAttribute{name, f64}, valid
		case "STRING", "string":
			return StringAttribute{name, string}, valid
		case "DATE", "date":
			if length == 4 {
				return DateAttribute{name, string, strings.clone(fields[3])}, valid
			}
			return DateAttribute{name, string, ""}, valid
		case "@RELATIONAL", "@relational":
			// TODO: Implement relational attributes
			fmt.println("Note :: Relational attributes are not yet supported")
			return Attribute{}, valid
		case:
			//Check if NOMINAL?
			field := fields[2]
			valid =
				valid &&
				field[0] == '{' &&
				field[len(field) - 1] == '}' &&
				strings.contains(field, ",")
			if valid {
				values := map[string]i64{}
				for value, idx in strings.split(
					field[1:len(field) - 1],
					",",
					context.temp_allocator,
				) {
					values[strings.clone(strings.trim(value, " \t\r\n"))] = i64(idx)
				}
				return NominalAttribute{name, i64, values}, valid
			}
		}
	}
	return Attribute{}, valid
}

read :: proc(
	filepath: string,
	allocator := context.allocator,
) -> (
	data: ARFFData,
	okay: bool = false,
) {
	/*******************************************************************/
	/* Setup the file handle and bufio structures for reading the file */
	/*******************************************************************/
	f, err := os.open(filepath)
	if err != nil {
		fmt.println("Error opening file: ", err)
		return data, okay
	}
	defer os.close(f)
	defer free_all(context.temp_allocator)

	r: bufio.Reader
	bufio.reader_init(&r, os.stream_from_handle(f), 1024, context.temp_allocator)
	defer bufio.reader_destroy(&r)
	bytes: []byte
	line: string

	/*******************************************/
	/* Parse the '@' values from the ARFF file */
	/*******************************************/
	read_rl, read_attr, read_data, read_inst: u64 = 0, 0, 0, 0
	parsed: bool = false
	for !parsed {
		// Read to the next ARFF value
		bytes, err = bufio.reader_read_bytes(&r, '@', context.temp_allocator)
		if err != nil {
			fmt.println("Error reading file while parsing header values :: ", err)
			return data, okay
		}
		err = bufio.reader_unread_byte(&r)
		line, err = bufio.reader_read_string(&r, '\n', context.temp_allocator)
		fields := arff_split(line, context.temp_allocator)

		// Parse the ARFF value
		switch tag := fields[0]; tag {
		case "@RELATION", "@relation":
			if read_rl >= 1 {
				fmt.println("Parsing Error :: Multiple relation declarations")
				return data, okay
			}
			if len(fields) != 2 {
				fmt.println("Parsing Error :: Invalid relation declaration :: ", line)
				fmt.println("See ", relation_reqs, " for more information.")
				return data, okay
			} else {
				data.relation = strings.clone(fields[1])
				read_rl += 1
			}
		case "@ATTRIBUTE", "@attribute":
			attribute, valid := create_validate_attribute(fields[:])
			if !valid {
				fmt.println("Parsing Error :: Invalid attribute declaration :: ", line)
				fmt.println("See ", attribute_reqs, " for more information.")
				return data, okay
			} else {
				append(&data.attributes, attribute)
				read_attr += 1
			}
		case "@DATA", "@data":
			if len(fields) != 1 {
				fmt.println("Parsing Error :: Invalid data declaration :: ", line)
				fmt.println("See ", data_reqs, " for more information.")
				return data, okay
			} else {
				parsed = true
				read_data += 1
			}
		case:
			fmt.println("Parsing Error :: Invalid tag :: ", line)
			fmt.println("See ", all_reqs, " for more information.")
			return data, okay
		}
	}

	/*************************************************************/
	/* Parse and load the data instances according to the header */
	/*************************************************************/
	for {
		line, err = bufio.reader_read_string(&r, '\n', context.temp_allocator)
		if err != nil {
			if err == os.ERROR_EOF {
				okay = true
				break
			} else {
				fmt.println("Error reading file while parsing data instances :: ", err)
				return data, okay
			}
		}
		values := strings.split(line[:len(line) - 1], ",", context.allocator)
		if len(values) != len(data.attributes) {
			fmt.println("Parsing Error :: Invalid number of values in data instance :: ", line)
			fmt.println("See ", instance_reqs, " for more information.")
			return data, okay
		}
		append(&data.data, values)
	}

	okay = true
	return data, okay
}
