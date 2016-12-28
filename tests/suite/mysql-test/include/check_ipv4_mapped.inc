# Check if ipv4 mapped to ipv6 is available.
#
# Parameters:
#  $check_ipv4_mapped_just_check - don't skip the test if IPv4 mapped is unsupported,
#                                  just set the variable $check_ipv4_mapped_supported
#
--disable_query_log
--disable_result_log
--disable_abort_on_error
let $check_ipv4_mapped_supported=1;
if($mysql_errno)
{
  let $check_ipv4_mapped_supported=0;
  if(!$check_ipv4_mapped_just_check)
  {
    skip No mapped IPv4 support;
  }
}
if(!$mysql_errno)
{
}
--enable_abort_on_error
--enable_result_log
--enable_query_log
# end check

