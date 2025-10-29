package shared

func RedWrap(s string) string {
	return "\033[31m" + s + "\033[0m"
}

func GreenWrap(s string) string {
	return "\033[32m" + s + "\033[0m"
}

func YellowWrap(s string) string {
	return "\033[33m" + s + "\033[0m"
}

func GreyWrap(s string) string {
	return "\033[90m" + s + "\033[0m"
}
