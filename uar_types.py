from attrs import frozen, field
from enum import Enum, auto
from typing import (
    TypedDict,
    TypeVar,
    NamedTuple,
    TYPE_CHECKING,
    Literal,
    Any,
    TYPE_CHECKING,
)

import pyparsing as pp
from pyparsing import pyparsing_common as pp_common


if TYPE_CHECKING:
    from uar import UARequestKey

pp.ParserElement.enablePackrat()
CURRENT_VERSION = (0, 1)


class UAFilterLiteral(NamedTuple):
    op: str
    negated: bool


class AliasedValue(NamedTuple):
    expr: str
    ua: UAFilterLiteral


class KeyRequestPair(NamedTuple):
    key: "UARequestKey"
    request: dict[Literal["reportRequests"], list[dict[str, Any]]]


class AliasedEnum(Enum):
    """Implements mappings between different representations of same enum

    This is useful when we have identifiers for different APIs / query forms
    that are semantically identical, but have different syntax. The idea is that
    this class defines a way of easily 'translating' between different
    representations of an identifier.

    If multiple enum values have the same value for a given key type, the first
    value declared will be returned.
    """

    @classmethod
    def get(cls, key_type: str, key_val: str):
        def key_matches(member: AliasedEnum) -> bool:
            return getattr(member.value, key_type) == key_val

        return next(filter(key_matches, cls))


@frozen
class VersionedParser:
    version_key: tuple[int, ...] = field(default=CURRENT_VERSION, kw_only=True)


class DateRangeJson(TypedDict):
    startDate: str
    endDate: str


class SamplingLevel(Enum):
    """Enum for describing sampling levels from the API

    Note that DEFAULT / MEDIUM both get mapped to the same number. The current
    implementation relies on the fact that when multiple names are mapped to the
    same value, that the first name declared having that value will be returned
    when <X>.name is called, where X is some instance of the enum.
    """

    SMALL = 1
    DEFAULT = 2
    MEDIUM = 2
    LARGE = 3


class ColumnType(Enum):
    DIMENSION = auto()
    METRIC = auto()


class FilterType(Enum):
    DIMENSION = auto()
    METRIC = auto()
    SEGMENT = auto()


class FilterOperator(AliasedEnum):
    EQ = AliasedValue("==", UAFilterLiteral("EXACT", False))
    NEQ = AliasedValue("!=", UAFilterLiteral("EXACT", True))
    IN = AliasedValue("IN", UAFilterLiteral("IN_LIST", False))
    GT = AliasedValue(">", UAFilterLiteral("GREATER_THAN", False))
    GTE = AliasedValue(">=", UAFilterLiteral("LESS_THAN", True))


def build_expr_parser() -> pp.ParserElement:
    Expression = pp.Forward().set_name("Expression")
    numeric_literal = pp_common.number
    string_literal = pp.QuotedString("'", esc_quote="''")
    literal_value = (
        numeric_literal
        | string_literal
        | pp.CaselessKeyword("TRUE")
        | pp.CaselessKeyword("FALSE")
        | pp.CaselessKeyword("NULL")
    )

    UNARY, BINARY, TERNARY = 1, 2, 3
    IN = pp.CaselessKeyword("IN")
    obj_ref = pp_common.identifier ^ pp.QuotedString('"', unquote_results=False)
    DOT, COMMA, LPAR, RPAR = map(pp.Suppress, ".,()")
    expr_term = (
        literal_value
        | pp.Group(obj_ref.set_name("table") + DOT + obj_ref.set_name("column"))
        | pp.Group(obj_ref.set_name("column"))
    )

    Expression <<= pp.infix_notation(  # type: ignore
        expr_term,
        [
            (pp.one_of("< > <= >="), BINARY, pp.OpAssoc.LEFT),
            (pp.one_of("= == <> !="), BINARY, pp.OpAssoc.LEFT),
            (
                IN + LPAR + pp.Group(pp.delimited_list(Expression)) + RPAR,
                UNARY,
                pp.OpAssoc.LEFT,
            ),
        ],
    )
    return Expression


Expression = build_expr_parser()
