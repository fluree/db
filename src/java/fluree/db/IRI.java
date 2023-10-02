package fluree.db;

import clojure.lang.IPersistentVector;

public final class IRI {
    private IRI() {}

    public static final long compare(IPersistentVector x, IPersistentVector y) {
        final int xCount = x.count();
        final int yCount = y.count();

        for(int i = 0; i < xCount; i++) {
            if(i < yCount) {
                final Long xi = (Long) x.nth(i);
                final Long yi = (Long) y.nth(i);
                int c = Long.compare(xi, yi);
                if(c != 0) {
                    return c;
                }
            } else {
                return 1;
            }
        }

        if (xCount < yCount) {
            return -1;
        } else {
            return 0;
        }
    }
}
