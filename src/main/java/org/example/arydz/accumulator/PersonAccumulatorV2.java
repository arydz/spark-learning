package org.example.arydz.accumulator;

import org.apache.spark.util.AccumulatorV2;

public class PersonAccumulatorV2 extends AccumulatorV2<Person, Person> {

    private Person person = new Person(15);

    @Override
    public boolean isZero() {
        return person != null;
    }

    @Override
    public AccumulatorV2<Person, Person> copy() {

        PersonAccumulatorV2 copy = new PersonAccumulatorV2();
        copy.merge(this);
        return copy;
    }

    @Override
    public void reset() {
        person.reset();
    }

    @Override
    public void add(Person v) {
        person.addAge(v.getAge());
    }

    @Override
    public void merge(AccumulatorV2<Person, Person> other) {
        Person value = other.value();
        person.addAge(value.getAge());
    }

    @Override
    public Person value() {
        return person;
    }
}
