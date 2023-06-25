package io.github.streamingwithflink.chapter5.datatype;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OriginDataType {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //long
        DataStream<Long> numbers = env.fromElements(1L, 2L, 3L, 4L, 5L);
        numbers.map(n->n+1).print();

        //tuple
        DataStream<Tuple2<String,Integer>> persons = env.fromElements(
                Tuple2.of("Tom", 12), Tuple2.of("Jack", 34), Tuple2.of("Alice", 23)
        );
        persons.filter(p->p.f1>20).print();
        //get person age
        DataStream<Integer> age = persons.map(p->p.f1);
        age.print();
        //set person Tom age as 15
        DataStream<Tuple2<String,Integer>> newPerson = persons.map(p->{
            if(p.f0.equals("Tom")){
                return Tuple2.of(p.f0,15);
            }else{
                return p;
            }
        });

        //use pojo
        DataStream<Person> persons2 = env.fromElements(
                new Person("Tom",12),new Person("Jack",34),new Person("Alice",23)
        );
        persons2.filter(p->p.age>20).print();
        env.execute();
    }
    //pojo
    public static class Person{
        public String name;
        public Integer age;
        public Person(){}
        public Person(String name,Integer age){
            this.name = name;
            this.age = age;
        }
        public String toString(){
            return name+":"+age;
        }
    }
}
