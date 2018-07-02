using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace RodenCore.Common
{
    public class HelperValidate
    {

        /// <summary>
        /// 邮箱正则
        /// </summary>
        public const string RegexEmail = @"^[a-zA-Z0-9]([a-zA-Z0-9]*[-_.]?[a-zA-Z0-9]+)*@([a-zA-Z0-9]*[-_]?[a-zA-Z0-9]+)+([.][a-zA-Z]{2,3})+$";
        /// <summary>
        /// 手机正则
        /// </summary>
        public const string RegexMobile = @"^(13|15|18|17)[0-9]{9}$";
        /// <summary>
        /// 固话正则
        /// </summary>
        public const string RegexPhone = @"^(\d{3,4}-?)?\d{7,8}$";
        /// <summary>
        /// IP正则
        /// </summary>
        public const string RegexIp = @"^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$";

        /// <summary>
        /// 判断是否是Email
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static bool IsEmail(string s)
        {
            return RegexMatch(s, RegexEmail);
        }


        /// <summary>
        /// 是否为手机号
        /// </summary>
        public static bool IsMobile(string s)
        {
            return RegexMatch(s, RegexMobile);
        }

        /// <summary>
        /// 是否为固话号
        /// </summary>
        public static bool IsPhone(string s)
        {
            return RegexMatch(s, RegexPhone);
        }

        /// <summary>
        /// 是否为IP
        /// </summary>
        public static bool IsIP(string s)
        {
            return RegexMatch(s, RegexIp);
        }

        private static bool RegexMatch(string s, string regex)
        {
            try
            {
                if (string.IsNullOrEmpty(s))
                    return false;

                Regex reg = new Regex(regex, RegexOptions.IgnoreCase | RegexOptions.Singleline, TimeSpan.FromSeconds(1));
                return reg.IsMatch(s);

            }
            catch (Exception e)
            {
                return false;
            }

        }
    }
}
