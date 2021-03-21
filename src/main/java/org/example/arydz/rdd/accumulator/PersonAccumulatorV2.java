package org.example.arydz.rdd.accumulator;

import org.apache.spark.util.AccumulatorV2;

public class PersonAccumulatorV2 extends AccumulatorV2<org.example.arydz.rdd.accumulator.Person, org.example.arydz.rdd.accumulator.Person> {

    private org.example.arydz.rdd.accumulator.Person person = new org.example.arydz.rdd.accumulator.Person(15);

    @Override
    public boolean isZero() {
        return person != null;
    }

    @Override
    public AccumulatorV2<org.example.arydz.rdd.accumulator.Person, org.example.arydz.rdd.accumulator.Person> copy() {

        PersonAccumulatorV2 copy = new PersonAccumulatorV2();
        copy.merge(this);
        return copy;
    }

    @Override
    public void reset() {
        person.reset();
    }

    @Override
    public void add(org.example.arydz.rdd.accumulator.Person v) {
        person.addAge(v.getAge());
    }

    @Override
    public void merge(AccumulatorV2<org.example.arydz.rdd.accumulator.Person, org.example.arydz.rdd.accumulator.Person> other) {
        org.example.arydz.rdd.accumulator.Person value = other.value();
        person.addAge(value.getAge());
    }

    @Override
    public org.example.arydz.rdd.accumulator.Person value() {
        return person;
    }
}
