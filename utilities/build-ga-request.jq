#!/usr/bin/env jq

# All input variables should be named exactly as described below
# Required variables:
#   - viewId
#   - dates
#   - dimensions
#   - metrics
#
# Optional arguments (NYI):
#   - dimensionFilters
#   - metricFilters

# DTYPES:

# Input is Object:
#   - viewId: Integer
#   - dates: DateClauseStr
#   - sampling?: Enum['SMALL', 'MEDIUM', 'LARGE']
#   - dimensions?: DimListStr
#   - dimensionFilters?: DimFilterStr
#   - pageSize?: Interval[1, 100k] & Integer
#   - hideValueRanges?: Boolean

# OptStr is: Identifier=ValueLiteral
# Identifier is String[LANG: ALNUM_LANG | OP_LANG | : | _]
# ValueLiteral is one of:
#   - Word
#   - SingleQuotedString
#   - DoubleQuotedString

# OP_LANG contains chars {+,-,*,/}
# ALNUM_LANG contains chars {[:alnum:]}
# WHITESPACE contains chars {[:space:]}
# WORD_LANG is Char / {',",WHITESPACE}

# DateClauseStr is one of:
#   - DateRangeStr
#   - DateRangeStr -o DateRangeStr
#
# DateRangeStr is: ISODate -to ISODate
# ISODate is: YYYY-MM-DD

# DimListStr is one of:
#   - DimStr
#   - DimStr DimListStr
#
# DimStr is one of:
#   - String
#   - String(DimOptStr)
#
# DimOptStr subtypes OptStr: Literal['buckets']=[BucketsValue]
# BucketsValue is one of:
#   - String
#   - String BucketsValue

#     viewId: (($ARGS.named["viewId"] // $input.viewId) | tostring),
#     dateRanges: $dates,
#     samplingLevel: ($ARGS.named["sampling"] // "LARGE"),
#     dimensions: $dimensions,
#     dimensionFilterClauses: (if ($dimensionFilters == null) then null else [$dimensionFilters] end),
#     pageSize: ($ARGS.named["pageSize"] // $input.pageSize // 100000),
#     hideValueRanges: ($ARGS.named["hideValueRanges"] // $input.hideValueRanges // false)

def doTwice(f): f | f;

def parseOpts(constructor):
  [
    capture("(?<arg>[[:alnum:]_:\\+\\-\\*\\/]+)(?<opts>[(][^)]*[)])?"; "g") | {
      arg, opts: ([
        .opts |
        if . == null then "" else .[1:-1] end |
        capture("(?<key>[^ =]+)=(?<value>[^ \"']+|\"(\\\"|[^\"])+\"|'[^']+')"; "g") |
        map_values(gsub("'"; ""))
      ] | from_entries ) } |
    constructor |
    with_entries(if .value==null then empty else . end)
  ];

def splitFilters($key):
  # Split filter list up on or / and conditionals

  # Uses -o (or) and -a (and) as the splitting clauses. The GA API requires that the entire
  # filter uses only one of OR or AND conditionals, not mixed clauses of both. Mixed clauses
  # should be rejected.
  ($ARGS.named[$key] // .[$key]) |
  if . then (
    split(" -o ") as $osplit |
    split(" -a ") as $asplit |
    if ((($osplit | length) > 1) and (($asplit | length) > 1)) then
      error("Filter clauses can only use one of -o or -a chaining, not both")
    elif (($asplit | length) > 1) then
      {operator: "AND", filters: $asplit}
    else
      {operator: "OR", filters: $osplit}
    end
  ) else null end;

def buildDimFilters($dfilter):
  $dfilter |
  "(?:(?<not>-not) )?" as $notRe |
  "(?:(?<case>-[iI]) )?" as $caseRe |
  "(?<dim>[a-zA-Z0-9_:]+) ?" as $dimRe |
  "(?<op>~=|\\^=|\\$=|\\*=|==|-eq|-lt|-gt|-in) ?" as $opRe |
  "(?<expr>[^ \"']+|\"[^\"]+\"|'[^']+'|[(][^)]+[)])$" as $exprRe |
  ($notRe + $caseRe + $dimRe + $opRe + $exprRe) as $dimFilterRe |
  # $dimFilterRe | debug |
  {
      "~=": "REGEXP",
      "^=": "BEGINS_WITH",
      "$=": "ENDS_WITH",
      "*=": "PARTIAL",
      "==": "EXACT",
      "-eq": "NUMERIC_EQUAL",
      "-gt": "GREATER_THAN",
      "-lt": "LESS_THAN",
      "-in": "IN_LIST"
  } as $dFilterOpTypes |

  $dfilter |
  .filters |= map(
    capture($dimFilterRe) |
    {
      dimensionName: .dim,
      "not": (.not != null),
      operator: $dFilterOpTypes[.op],
      expressions: (
        if .op != "-in" then [
          .expr |
          gsub("[\"']"; "")
        ]
        else (
          .expr[1:-1] |
          [
            scan("([^ \"']+|\"[^\"]+\"|['][^']+['])")[]
            | gsub("[\"']"; "")
          ]
        )
        end
      ),
      caseSensitive: (.case == "-I")
    }
  );

def build:
  . as $input |

  $ARGS.named["dimensions"] // $input.dimensions |
  parseOpts(
    { name: .arg, histogramBuckets: (.opts.buckets[1:-1]? | split(" ")? // null) }
  ) as $dimensions |

  $ARGS.named["metrics"] // $input.metrics |
  split(" -a ") |
  map(parseOpts(
    { expression: .arg, alias: .opts.alias, formattingType: .opts.format }
  )) as $metrics |

  # args
  $ARGS.named["goals"] // $input.goals |

  $ARGS.named["dates"] // $input.dates |
  split(" -o ")[:2] |
  map(split(" -to ") | {startDate: .[0], endDate: .[1]}) as $dates |

  $input |
  splitFilters("dimensionFilters") |
  if . then buildDimFilters(.)
  else null end |
  . as $dimensionFilters |
  {
      viewId: (($ARGS.named["viewId"] // $input.viewId) | tostring),
      dateRanges: $dates,
      samplingLevel: ($ARGS.named["sampling"] // "LARGE"),
      dimensions: $dimensions,
      dimensionFilterClauses: (if ($dimensionFilters == null) then null else [$dimensionFilters] end),
      pageSize: ($ARGS.named["pageSize"] // $input.pageSize // 100000),
      hideValueRanges: ($ARGS.named["hideValueRanges"] // $input.hideValueRanges // false)
  } |
  with_entries(if .value == null then empty else . end) |
  .metrics = $metrics[];

if (type | . == "array") then map(build) else build end |
map({reportRequests: [.]})[]
