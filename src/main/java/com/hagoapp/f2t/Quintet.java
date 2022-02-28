/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.hagoapp.f2t;

public class Quintet<A, B, C, D, E> extends Quartet<A, B, C, D> {
    private E fifth;

    public Quintet(A a, B b, C c, D d, E e) {
        super(a, b, c, d);
        fifth = e;
    }

    public E getFifth() {
        return fifth;
    }

    public void setFifth(E fifth) {
        this.fifth = fifth;
    }

    @Override
    public String toString() {
        return "Quartet{" +
                "first=" + getFirst() +
                ", second=" + getSecond() +
                ", third=" + getThird() +
                ", fourth=" + getFourth() +
                "fifth=" + fifth +
                '}';
    }
}
