package org.example.arydz.accumulator;

import java.io.Serializable;

public class Person implements Serializable {

    private static final long serialVersionUID = -3493847518958769131L;

    public Person(int age) {
        this.age = age;
    }

    private int age;

    public void reset() {
        age = 0;
    }

    public int getAge() {
        return age;
    }

    public void addAge(int value) {
        age += value;
    }

    @Override
    public String toString() {
        return "Person{" +
                "age=" + age +
                '}';
    }
}
