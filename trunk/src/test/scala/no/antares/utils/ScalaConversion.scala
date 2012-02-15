package no.antares.utils

/** @author tommyskodje */

class ScalaConversion[T] {
  def varArg( scripts: Array[T] ) : Seq[T] = scripts.toSeq;
}
object ScalaConversion {
}