package com.example.workshop;

import io.reactivex.Observable;

public class FlatMapOperatorExample {
    public static void main(String[] args) {
        Observable.just(1, 2, 3)
                .flatMap(item -> Observable.just(item * 10, item * 20))
                .subscribe(System.out::println);
    }
}
