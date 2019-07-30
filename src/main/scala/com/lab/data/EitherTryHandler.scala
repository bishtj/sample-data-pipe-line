package com.lab.data

import scala.util.{Failure, Success, Try}

trait EitherTryHandler {

  def eitherR[T](tryResult: Try[T]): Either[AnaError, T] = {

    tryResult match {
      case Failure(e) => Left(AnaError(e.toString))
      case Success(v) => Right(v)
    }
  }
}
