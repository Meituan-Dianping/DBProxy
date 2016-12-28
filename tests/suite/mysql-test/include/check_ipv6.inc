# Check if ipv6 is available.
#
# Parameters:
#  $check_ipv6_just_check - don't skip the test if IPv6 is unsupported,
#                           just set the variable $check_ipv6_supported
#
--disable_query_log
--disable_result_log
--disable_abort_on_error
let $check_ipv6_supported=1;
if($mysql_errno)
{
  let $check_ipv6_supported=0;
  if(!$check_ipv6_just_check)
  {
    skip No IPv6 support;
  }
}
if(!$mysql_errno)
{
}
--enable_abort_on_error
--enable_result_log
--enable_query_log
# end check

