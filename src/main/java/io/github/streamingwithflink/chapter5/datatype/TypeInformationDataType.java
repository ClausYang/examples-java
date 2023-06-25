package io.github.streamingwithflink.chapter5.datatype;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import io.github.streamingwithflink.chapter5.datatype.OriginDataType.Person;

public class TypeInformationDataType {
    //orginal typeinformation
    TypeInformation<Integer> intType = Types.INT;
    TypeInformation<String> stringType = Types.STRING;

    //tuple
    TypeInformation<Tuple2<String,Integer>> tupleType = Types.TUPLE(Types.STRING,Types.INT);

    //pojo
    TypeInformation<Person> personType = Types.POJO(Person.class);
}
