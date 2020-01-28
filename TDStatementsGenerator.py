import avro
import sys
import tempfile
import google
import urllib2
from google.protobuf import reflection as _reflection
from avro import schema
from google.protobuf import descriptor_pb2
from google.protobuf import text_format
from google.protobuf import descriptor
from google.protobuf.descriptor import FileDescriptor


def addObjsForSingleField(prefix, field, proto_schema_fields_names_to_numbers_map, single_fields_nums_arr,
                          proto_schema_fields_numbers_to_generated_objects):
    name = prefix + field.name
    fieldNum = proto_schema_fields_names_to_numbers_map.get(name)
    single_fields_nums_arr.append(fieldNum)
    objs = []
    objs.append(name)
    objs.append(str(fieldNum) + "-0")
    type = getType2(field.type)
    objs.append(type)
    proto_schema_fields_numbers_to_generated_objects[fieldNum] = objs


def generateFieldsWithTags(avro_text, proto_schema_fields, module_name, version):
    multi_fields_table_creation_statements = []
    multi_fields_view_creation_statements = []
    id_field_number = None
    proto_schema_fields_names_to_numbers_map = {}
    proto_schema_fields_numbers_to_generated_objects = {}
    for field in proto_schema_fields:

        proto_schema_fields_names_to_numbers_map[field.name] = field.number
        if (field.name == "id"):
            id_field_number = str(field.number) + "-0"

    avro_schema = schema.parse(avro_text)
    single_fields_nums_arr = []
    view_name = "rtpoc_{0}_{1}_multi_value_staging_vw".format(version, module_name)
    procedure_name = "rtpoc_{0}_{1}_a_sp".format(version, module_name)
    sp_insert_select_statements = ""
    for field in avro_schema.fields:

        sc, parent = checkRecordSchema(field)
        if (sc != None and field.name != "metadata"):

            fields_nums_arr = []
            fields_names = ""
            fields_nums = ""
            fields_and_types = ""
            value_field_number = ""
            for structField in sc.fields:
                name = ""
                fieldNum = 0
                fieldNum_str = ""
                type = "varchar(300)"
                elias = ""
                if structField.name == "VALUE":
                    name = "c_" + field.name
                    # name=field.name
                    fieldNum = proto_schema_fields_names_to_numbers_map.get(field.name)
                    value_field_number = str(fieldNum)
                    elias = "v"
                else:
                    name = field.name + "_" + structField.name
                    fieldNum = proto_schema_fields_names_to_numbers_map.get(name)
                    elias = structField.name
                fields_nums_arr.append(fieldNum)
                fieldNum_str = str(fieldNum)
                if parent == "array":
                    fieldNum_str = fieldNum_str + "-1"
                else:
                    fieldNum_str = fieldNum_str + "-0"

                a = []
                a.append(name)
                a.append(fieldNum_str)
                a.append(type)
                a.append(elias)
                proto_schema_fields_numbers_to_generated_objects[fieldNum] = a

            fields_nums_arr.sort()
            fields_names = "offset, id ,"
            fields_names_with_elias = "offset,id ,";
            fields_nums = str(id_field_number) + ";"
            fields_and_types = "offset varchar(200) , id varchar(200) ,"
            for num in fields_nums_arr:
                objs = proto_schema_fields_numbers_to_generated_objects.get(num)
                fields_names = fields_names + objs[0] + ","
                fields_names_with_elias = fields_names_with_elias + objs[3] + " as " + objs[0] + " ,"
                fields_nums = fields_nums + objs[1] + ";"
                fields_and_types = fields_and_types + objs[0] + " " + objs[2] + " ,"

            fields_names = fields_names.rstrip(",")
            fields_names_with_elias = fields_names_with_elias.rstrip(",")
            fields_and_types = fields_and_types.rstrip(",")
            table_name = "rtpoc_{0}_{1}_{2}".format(version, module_name, field.name)
            print("drop table " + table_name + ";");
            view_name = "rtpoc_{0}_{1}_{2}_vw".format(version, module_name, field.name)
            other_view_name = "rtpoc_{0}_{1}_multi_value_staging_vw".format(version, module_name)
            create_table_statement = "create multiset table {0} ({1});".format(table_name, fields_and_types)
            multi_fields_table_creation_statements.append(create_table_statement)
            # create_view_statement="create view {0} \n as select {1} from {2} ( __fnc__( '{3}' , rtpoc_{4}_land.message_bytes, rtpoc_{4}_land.partition_offset) returns ( {5} )) as q1;".format(view_name ,fields_names,"table",fields_nums,module_name,fields_and_types )
            create_view_statement = "replace view {0} \n as select {1} from {2} where VALUE_TAG = {3} ;".format(
                view_name, fields_names_with_elias, other_view_name, value_field_number)
            multi_fields_view_creation_statements.append(create_view_statement)
            procedure_name = "rtpoc_{0}_{1}_{2}_sp".format(version, module_name, field.name)
            sp_creation_statement = "create procedure {0} () \n begin \n insert into {1} ( {2} ) select {3} from {4}; \n end;".format(
                procedure_name, table_name, fields_names, fields_names, view_name)
            sp_insert_select_statement = "insert into {0} ( {1} ) select {2} from {3}; \n ".format(table_name,
                                                                                                   fields_names,
                                                                                                   fields_names,
                                                                                                   view_name)
            sp_insert_select_statements = sp_insert_select_statements + sp_insert_select_statement
            # multi_fields_SP_creation_statements.append(sp_creation_statement)
            # print("--statements for "+field.name)
            # print("\n")
            # print("fields names "+ fields_names)
            # print("fields nums "+ fields_nums)
            # print("fields and types "+ fields_and_types)
            print(create_table_statement + "\n")
            print(create_view_statement)
            print(sp_creation_statement)
            print(
                "--===============================================================================================================================\n")
        else:

            if (field.name == "metadata"):
                for subField in sc.fields:
                    addObjsForSingleField("metadata_", subField, proto_schema_fields_names_to_numbers_map,
                                          single_fields_nums_arr, proto_schema_fields_numbers_to_generated_objects)
            else:
                addObjsForSingleField("", field, proto_schema_fields_names_to_numbers_map, single_fields_nums_arr,
                                      proto_schema_fields_numbers_to_generated_objects)

    single_fields_nums_arr.sort()
    single_fields_names = "offset , "
    single_fields_nums = ""
    single_fields_and_types = "offset varchar(200), "
    for num in single_fields_nums_arr:
        objs = proto_schema_fields_numbers_to_generated_objects.get(num)
        single_fields_names = single_fields_names + objs[0] + ","
        single_fields_nums = single_fields_nums + objs[1] + ";"
        single_fields_and_types = single_fields_and_types + objs[0] + " " + objs[2] + " ,";

    single_fields_names = single_fields_names.rstrip(",")
    single_fields_and_types = single_fields_and_types.rstrip(',')
    single_table_name = "rtpoc_{0}_{1}_single".format(version, module_name)
    single_view_name = "rtpoc_{0}_{1}_single_vw".format(version, module_name)
    single_create_table_statement = "create multiset table {0} ( {1} ); ".format(single_table_name,
                                                                                 single_fields_and_types)
    print("drop table " + single_table_name + ";")
    print(single_create_table_statement)
    single_view_table_statement = "create view {0} \n as select {1} from {2} ( __fnc__( '{3}' , rtpoc_{4}_land.message_bytes, rtpoc_{4}_land.partition_offset) returns ({5} )) as q1;".format(
        single_view_name, single_fields_names, "table", single_fields_nums, module_name, single_fields_and_types)
    print(single_view_table_statement)
    single_procedure_name = "rtpoc_{0}_{1}_single_ps".format(version, module_name)
    single_sp_creation_statement = "create procedure {0} () \n begin \n insert into {1} ( {2} ) select {3} from {4} ; \n end;".format(
        single_procedure_name, single_table_name, single_fields_names, single_fields_names, single_view_name)

    print(single_sp_creation_statement)
    single_sp_insert_select_statement = "insert into {0} ( {1} ) select {2} from {3};\n".format(single_table_name,
                                                                                                single_fields_names,
                                                                                                single_fields_names,
                                                                                                single_view_name)
    sp_insert_select_statements = sp_insert_select_statements + single_sp_insert_select_statement
    procedure_create_statement = "create procedure {0} () \n begin \n {1} \n end;".format(procedure_name,
                                                                                          sp_insert_select_statements)
    # print "--================================================================================================================================ \n"
    # print procedure_create_statement


def generateSchemaHiveStatements(avro_text):
    avro_schema = schema.parse(avro_text)

    single_fields_for_select = ""
    single_fields_for_create = ""

    for field in avro_schema.fields:
        sc, parent = checkRecordSchema(field)
        if (sc != None):
            fields_for_create = ""
            fields_for_select = ""
            for structField in sc.fields:
                name = field.name + "_" + structField.name
                if fields_for_select != "":
                    fields_for_select = fields_for_select + "," + name
                else:
                    fields_for_select = name

                fields_for_create = fields_for_create + name + " string,\n"

            fields_for_create = fields_for_create.rstrip(",\n")
            fields_for_select = fields_for_select.rstrip(",")
            create_statement = "create table {0} ( {1} )".format(field.name, fields_for_create)
            select_statement = "select {0} from {1} ".format(fields_for_select, field.name)
            print("statements for " + field.name)
            print(create_statement)
            print(select_statement)
            print(
                "=============================================================================================================")
        else:

            single_fields_for_select = single_fields_for_select + field.name + ","
            type = getType2(field.type)
            if "array" in type:
                a = type.split(' ')
                type = "array<{0}>".format(a[0])
            single_fields_for_create = single_fields_for_create + field.name + " " + type + ",\n"

    print("single statements ")
    print "create table account ( {0} ) ".format(single_fields_for_create)
    print "select {0} from account ".format(single_fields_for_select)


def checkRecordSchema(field):
    type = field.type
    sc = None
    parent = None

    if isinstance(type, schema.UnionSchema):

        type = type._schemas[1]
        if isinstance(type, schema.ArraySchema) and isinstance(type.items, schema.RecordSchema):
            sc = type.items
            parent = "array"
        elif isinstance(type, schema.RecordSchema):
            sc = type
            parent = "union"
    elif isinstance(type, schema.ArraySchema) and isinstance(type.items, schema.RecordSchema):
        sc = type.items
        parent = "array"
    elif isinstance(type, schema.RecordSchema):
        sc = type
        parent = None

    return sc, parent


def getType2(typ):
    if isinstance(typ, schema.UnionSchema):
        return getType2(typ._schemas[1])

    if (isinstance(typ, schema.ArraySchema)):
        return getType2(typ.items) + " array"

    # if(isinstance(typ,schema.RecordSchema)):
    #      print("inside getType 2 ", typ, "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&*****************")
    # return getORAddStruct(messages,typ)
    if (isinstance(typ, schema.MapSchema)):
        # valType= getType2(typ.values, messages)
        return "map < varchar(200), varchar(200) > "
    else:

        val = typ.fullname.replace("\"", "", -1)
        if val == "int":
            return "varchar(200)"
        elif val == "long":
            return "varchar(200)"
        elif val == "double":
            return "varchar(200)"
        elif val == "string":
            return "varchar(200)"
        return val


if __name__ == "__main__":
    sc1 = urllib2.urlopen(sys.argv[1]).read()
    protoText = urllib2.urlopen(sys.argv[2]).read()
    module_name = sys.argv[3]
    version = sys.argv[4]
    # call proxy get proto text
    # previousSchemaProtoText=
    file_descriptor_proto = descriptor_pb2.FileDescriptorProto()
    text_format.Merge(protoText, file_descriptor_proto)
    protoSchema = file_descriptor_proto
    proto_schema_fields = protoSchema.message_type[0].field
    generateFieldsWithTags(sc1, proto_schema_fields, module_name, version)
    # generateSchemaHiveStatements(sc1)