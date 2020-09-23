function* foo(m) {
  while (true) {
    let v = yield;
    yield v;
  }
}

var it = foo();
it.next(0);
console.log(it.next(1));
it.next();
console.log(it.next(2));
it.next();
console.log(it.next(3));
