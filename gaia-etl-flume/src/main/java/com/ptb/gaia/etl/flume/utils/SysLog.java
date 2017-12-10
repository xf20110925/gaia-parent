package com.ptb.gaia.etl.flume.utils;

import com.ptb.utils.log.LogUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by eric on 16/6/15.
 */
public class SysLog {
    final static String ComponetName = "gaia-etl-flume";
    static Logger log = LoggerFactory.getLogger(SysLog.class);

    public static class Action {
        public static int ACCESS = 100000;
        public static int FLUME_SINK_PROCESS = 100001;
        public static int FLUME_SINK_PROCESS_ERROR = 110000;
        int code;
        String message;
        public static Action Null = new Action(0, "null");

        public Action(int code, String message) {
            this.code = code;
            this.message = message;
        }

        public Action() {
        }

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return String.join(":::", String.valueOf(code), message);
        }
    }

    public static void log(Action action, Action errorAction) {
        LogUtils.ActionResult actionResult = errorAction == null ? LogUtils.ActionResult.success : LogUtils.ActionResult.failed;
        if (errorAction == null) {
            errorAction = Action.Null;
        }
        LogUtils.logInfo(ComponetName, action.toString(), actionResult, errorAction.toString());
    }

    public static void info(Action action) {
        log(action, null);
    }

    public static void error(Action action, int code, String message) {
        log(action, new Action(code, message));
    }

}
