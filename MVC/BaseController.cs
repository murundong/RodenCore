using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Mvc;
using System.Web.Routing;

namespace RodenCore.MVC
{
    public class BaseController : Controller
    {


        /// <summary>
        /// 弹出alert并刷新页面
        /// </summary>
        /// <param name="alert"></param>
        /// <returns></returns>
        public ContentResult RefreshParent(string alert = null)
        {
            var script = $"<script>{(string.IsNullOrWhiteSpace(alert) ? string.Empty : $"alert('{alert}')")}; parent.location.reload(1)</script>";
            return Content(script);
        }

        /// <summary>
        /// 弹出alert，关闭页面，并且刷新前一个页面
        /// </summary>
        /// <param name="alert"></param>
        /// <returns></returns>
        public ContentResult ClosedAndRefreshOpener(string alert = null)
        {

            var script = $"<script>{(string.IsNullOrWhiteSpace(alert) ? string.Empty : $"alert('{alert}')")}; window.opener.location.reload(1);window.opener = null;window.open('', '_parent', '');window.close();</script>";
            return Content(script);
            
        }


        /// <summary>
        /// 执行页面的script方法
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="ps"></param>
        /// <returns></returns>
        public ContentResult ExecuteScript(string fun, params object[] ps)
        {
            var script = $"<script>{fun}({string.Join(",", ps.Select(s => JsonConvert.SerializeObject(s)))})</script>";
            return Content(script);
            
        }
    }
}
