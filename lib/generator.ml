type result = Done | Value of Tuple.t * (int option -> result) | Error of string
and t = int option -> result
