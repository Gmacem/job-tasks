package counter

import (
	"fmt"
	"strconv"
	"strings"
)

func ipToUint32(ip string) (uint32, error) {
	parts := strings.Split(ip, ".")
	if len(parts) != 4 {
		return 0, fmt.Errorf("invalid IP address format: %s", ip)
	}

	var result uint32
	for i := 0; i < 4; i++ {
		num, err := strconv.Atoi(parts[i])
		if err != nil {
			return 0, fmt.Errorf("invalid IP address part: %s", parts[i])
		}
		if num < 0 || num > 255 {
			return 0, fmt.Errorf("IP address part out of range: %d", num)
		}
		result |= uint32(num) << uint((3-i)*8)
	}
	return result, nil
}
