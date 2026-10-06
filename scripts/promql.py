"""Source metadata joins for the check scripts' PromQL subset.

Only selectors and rate(selector[window]) are rewritten. Applying rate before
joining preserves counter reset semantics and avoids range-vector subqueries.
This is deliberately not a general-purpose PromQL parser.
"""
import re

SOURCE_JOBS = 'uipath/(uipath-orchestrator|uipath-otel-adapter)'
SELECTOR = r'uipath_[a-zA-Z0-9_]+\{(?:[^"{}]|"(?:\\.|[^"\\])*")*\}'
TERM = re.compile(r'rate\((?P<counter>' + SELECTOR + r')\[(?P<window>[^\]]+)\]\)|(?P<gauge>' + SELECTOR + r')')
INSTALLATION = re.compile(r'uipath_installation(?:=~|=)"(?:\\.|[^"\\])*",?')


def with_source_metadata(expression):
    def enrich(match):
        selector = match['counter'] or match['gauge']
        source_filter = INSTALLATION.search(selector)
        if source_filter is None:
            raise ValueError('UiPath metric selector must specify installation')
        source_filter = source_filter.group().rstrip(',')
        selector = INSTALLATION.sub('', selector).replace(',}', '}')
        vector = f'rate({selector}[{match["window"]}])' if match['counter'] else selector
        info = (f'max by (job,instance,uipath_installation,uipath_tenant_id) '
                f'(target_info{{job=~"{SOURCE_JOBS}",{source_filter}}})')
        return f'({vector} * on (job,instance) group_left(uipath_installation,uipath_tenant_id) ({info}))'
    return TERM.sub(enrich, expression)
