#undef NDEBUG
#include <string>
#include <iostream>
#include <assert.h>
#include <vector>
#include "Transaction.hh"
#include "TArray.hh"
#include "TBox.hh"
#include <time.h>

inline double gettime_d() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec + ts.tv_nsec / 1.0e9;
}

void testSimpleInt() {
	TArray<int, 100> f;

    {
        TransactionGuard t;
        f[1] = 100;
    }

	{
        TransactionGuard t2;
        int f_read = f[1];
        assert(f_read == 100);
    }

	printf("PASS: %s\n", __FUNCTION__);
}

void testSimpleString() {
	TArray<std::string, 100> f;

	{
        TransactionGuard t;
        f[1] = "100";
    }

	{
        TransactionGuard t2;
        std::string f_read = f[1];
        assert(f_read.compare("100") == 0);
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testIter() {
    std::vector<int> arr;
    TArray<int, 10> f;
    for (int i = 0; i < 10; i++) {
        int x = rand();
        arr.push_back(x);
        f.nontrans_put(i, x);
    }
    int max;
    TRANSACTION {
        max = *(std::max_element(f.begin(), f.end()));
    } RETRY(false);

    assert(max == *(std::max_element(arr.begin(), arr.end())));
    printf("Max is %i\n", max);
    printf("PASS: %s\n", __FUNCTION__);
}

void testTicToc0() {
    TArray<int, 10, TNonopaqueWrapped> f;
    for (int i = 0; i < 10; ++i) {
        f.nontrans_put(i, i);
    }

    {
        TestTransaction t1(1);
        int a = f[4];
        assert(a == 4);

        TestTransaction t2(2);
        f[4] = 5;
        assert(t2.try_commit());

        t1.use();
        a = f[5];
        assert(a == 5);
        assert(!t1.try_commit());
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testConflictingIter() {
    TArray<int, 10> f;
    TBox<int> box;
    for (int i = 0; i < 10; i++) {
        f.nontrans_put(i, i);
    }

    {
        TestTransaction t(1);
        std::max_element(f.begin(), f.end());
        box = 9; /* avoid read-only txn */

        TestTransaction t1(2);
        f[4] = 10;
        assert(t1.try_commit());
        assert(!t.try_commit());
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testModifyingIter() {
    TArray<int, 10> f;
    for (int i = 0; i < 10; i++)
        f.nontrans_put(i, i);

    {
        TransactionGuard t;
        std::replace(f.begin(), f.end(), 4, 6);
    }

    {
        TransactionGuard t1;
        int v = f[4];
        assert(v == 6);
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testConflictingModifyIter1() {
    TArray<int, 10> f;
    for (int i = 0; i < 10; i++)
        f.nontrans_put(i, i);

    {
        TestTransaction t(1);
        std::replace(f.begin(), f.end(), 4, 6);

        TestTransaction t1(2);
        f[4] = 10;

        assert(t1.try_commit());
        assert(!t.try_commit());
    }

    {
        TransactionGuard t2;
        int v = f[4];
        assert(v == 10);
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testConflictingModifyIter2() {
    TArray<int, 10> f;
    for (int i = 0; i < 10; i++)
        f.nontrans_put(i, i);

    {
        TransactionGuard t;
        std::replace(f.begin(), f.end(), 4, 6);
    }

    {
        TransactionGuard t1;
        f[4] = 10;
    }

    {
        TransactionGuard t2;
        int v = f[4];
        assert(v == 10);
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testConflictingModifyIter3() {
    TArray<int, 10> f;
    TBox<int> box;
    for (int i = 0; i < 10; i++)
        f.nontrans_put(i, i);

    {
        TestTransaction t1(1);
        int x = f[4];
        assert(x == 4);
        box = 9; /* avoid read-only txn */

        TestTransaction t(2);
        std::replace(f.begin(), f.end(), 4, 6);

        assert(t.try_commit());
        assert(!t1.try_commit());
    }

    {
        TransactionGuard t2;
        int v = f[4];
        assert(v == 6);
    }

    printf("PASS: %s\n", __FUNCTION__);
}


void testOpacity1() {
    TArray<int, 10> f;
    for (int i = 0; i < 10; i++)
        f.nontrans_put(i, i);

    try {
        TestTransaction t1(1);
        int x = f[3];
        assert(x == 3);

        TestTransaction t(2);
        f[3] = 2;
        std::replace(f.begin(), f.end(), 4, 6);
        assert(t.try_commit());

        t1.use();
        x = f[4];
        assert(false && "shouldn't get here");
        assert(x == 6);
        assert(!t1.try_commit());
    } catch (Transaction::Abort e) {
    }

    {
        TransactionGuard t2;
        int v = f[4];
        assert(v == 6);
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void testNoOpacity1() {
    TArray<int, 10, TNonopaqueWrapped> f;
    for (int i = 0; i < 10; i++)
        f.nontrans_put(i, i);

    {
        TestTransaction t1(1);
        int x = f[3];
        assert(x == 3);

        TestTransaction t(2);
        f[3] = 2;
        std::replace(f.begin(), f.end(), 4, 6);
        assert(t.try_commit());

        t1.use();
        x = f[4];
        assert(x == 6);
        assert(!t1.try_commit());
    }

    {
        TransactionGuard t2;
        int v = f[4];
        assert(v == 6);
    }

    printf("PASS: %s\n", __FUNCTION__);
}

void benchArray64() {
    TArray<int, 64> a;
    for (int i = 0; i < 64; ++i)
        a.nontrans_put(i, 0);

    const unsigned long niters = 1000;
    double before = gettime_d();
    for (unsigned long iter = 0; iter < niters; ++iter) {
        for (int j = 0; j < 1000; ++j) {
            TRANSACTION {
                for (int i = 0; i < 64; ++i)
                    a[i] = a[i] + i;
            } RETRY(true);
        }
    }
    double after = gettime_d();

    printf("NS PER ITER (iter = 1000tx): %g\n", (after - before) * 1.0e9 / niters);
}

int main() {
    testSimpleInt();
    testSimpleString();
    testIter();
    testTicToc0();
    testConflictingIter();
    testModifyingIter();
    testConflictingModifyIter1();
    testConflictingModifyIter2();
    testConflictingModifyIter3();
    testOpacity1();
    testNoOpacity1();
    benchArray64();
    return 0;
}
