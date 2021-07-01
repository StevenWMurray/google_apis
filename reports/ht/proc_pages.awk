#!/usr/bin/env gawk -E

NR == 1 {
    for (i=2; i < NF; i++)
        $i = $(i+2)
    NF -= 2
    $0 = $0
    print $0
}
NR > 1 {
    sub(/\?.*"$/, "\"", $1)
    for (i = 1; i <= 4; i++)
        totals[$1 OFS $4][i] += $(i+4)
}
END {
    for (key in totals) {
        if (totals[key][2] >= 10) {
            printf "%s%s", key, OFS
            for (i = 1; i <= 2; i++)
                printf "%d%s", totals[key][i], OFS
            for (i = 3; i <= 4; i++)
                printf "%.3f%s", totals[key][i] / totals[key][2] / 1000, OFS
            printf "%s", ORS
        }
    }
}
