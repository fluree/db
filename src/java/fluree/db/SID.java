package fluree.db;

import java.lang.Comparable;

public final class SID implements Comparable {
    final int namespaceCode;
    final long[] nameCodes;

    public SID(int nsCode, long... nameCodes) {
        this.namespaceCode = nsCode;
        this.nameCodes = nameCodes;
    }

    public int getNamespaceCode() {
        return namespaceCode;
    }

    public long[] getNameCodes() {
        return nameCodes;
    }

    public final int compareTo(Object o) {
        SID that = (SID) o;
        int nsComp = Integer.compare(this.getNamespaceCode(), that.getNamespaceCode());
        if (nsComp != 0) {
            return nsComp;
        }

        final long[] theseCodes = this.getNameCodes();
        final int thisLength = theseCodes.length;

        final long[] thoseCodes = that.getNameCodes();
        final int thatLength = thoseCodes.length;

        for(int i = 0; i < thisLength; i++) {
            if(i < thatLength) {
                final long thisI = theseCodes[i];
                final long thatI = thoseCodes[i];
                int c = Long.compare(thisI, thatI);
                if (c != 0) {
                    return c;
                }
            } else {
                return 1;
            }
        }

        if (thisLength < thatLength) {
            return -1;
        } else {
            return 0;
        }
    }

    public final boolean equals(Object o) {
        if (o instanceof SID) {
            SID that = (SID) o;
            return this.compareTo(that) == 0;
        }
        return false;
    }

    public final int hashCode() {
        int hsh = this.getNamespaceCode();
        final long[] nameCodes = this.getNameCodes();

        for(int i = 0; i < nameCodes.length; i++) {
            hsh += Long.hashCode(nameCodes[i]);
        }

        return hsh;
    }

    public static final int compare(SID s1, SID s2) {
        return s1.compareTo(s2);
    }
}
