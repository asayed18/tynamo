enum LogLevel {
    DEBUG,
    INFO,
    WARN,
    ERROR,
    OFF
}

const LogNameMap = {
    DEBUG: LogLevel.DEBUG,
    INFO: LogLevel.INFO,
    WARN: LogLevel.WARN,
    ERROR: LogLevel.ERROR,
    OFF: LogLevel.OFF,
}

type LogName = keyof typeof LogNameMap

class Logger {
    private currentLevel: LogLevel

    constructor(level: LogName) {
        this.currentLevel = LogNameMap[level]
    }

    log(level: LogLevel, ...data: any[]): void {
        if (level >= this.currentLevel) {
            switch (level) {
                case LogLevel.DEBUG:
                    console.debug(...data)
                    break
                case LogLevel.INFO:
                    console.info(...data)
                    break
                case LogLevel.WARN:
                    console.warn(...data)
                    break
                case LogLevel.ERROR:
                    console.error(...data)
                    break
                default:
                    break
            }
        }
    }

    setLevel(level: LogLevel): void {
        this.currentLevel = level
    }
}

export { Logger, LogLevel, LogName, LogNameMap }