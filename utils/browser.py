from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from user_agents import parse
def parse_browser(ua):
    user_agent = parse(ua)
    return user_agent.browser.family


parse_browser_udf = udf(parse_browser, returnType=StringType())