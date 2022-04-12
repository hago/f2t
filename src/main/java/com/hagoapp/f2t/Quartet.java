/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

/**
 * A container for 4 associated objects.
 *
 * @param <A> first object
 * @param <B> second object
 * @param <C> third object
 * @param <D> fourth object
 */
public class Quartet<A, B, C, D> {
    private A first;
    private B second;
    private C third;
    private D fourth;

    public Quartet() {
        //
    }

    public Quartet(A a, B b, C c, D d) {
        first = a;
        second = b;
        third = c;
        fourth = d;
    }

    public A getFirst() {
        return first;
    }

    public void setFirst(A first) {
        this.first = first;
    }

    public B getSecond() {
        return second;
    }

    public void setSecond(B second) {
        this.second = second;
    }

    public C getThird() {
        return third;
    }

    public void setThird(C third) {
        this.third = third;
    }

    public D getFourth() {
        return fourth;
    }

    public void setFourth(D fourth) {
        this.fourth = fourth;
    }

    @Override
    public String toString() {
        return "Quartet{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                ", fourth=" + fourth +
                '}';
    }
}
