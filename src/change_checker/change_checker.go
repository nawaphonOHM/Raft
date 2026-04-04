package change_checker

func IsChange[T interface{}](ch <-chan T) bool {

	if ch == nil {
		return false
	}

	select {
	case _, open := <-ch:
		{

			if !open {
				return false
			}
			return true
		}
	default:
		return false
	}

}
