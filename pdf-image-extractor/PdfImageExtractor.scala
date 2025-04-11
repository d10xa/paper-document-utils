//> using scala "3.3.1"
//> using dep "org.typelevel::cats-effect:3.6.1"
//> using dep "co.fs2::fs2-core:3.12.0"
//> using dep "co.fs2::fs2-io:3.12.0"
//> using dep "com.monovore::decline:2.5.0"
//> using dep "com.monovore::decline-effect:2.5.0"

import cats.effect.*
import cats.implicits.*
import cats.effect.syntax.concurrent.*
import com.monovore.decline.*
import com.monovore.decline.effect.*
import fs2.*
import fs2.io.file.*
import fs2.io.process.*


object PdfImageExtractor extends CommandIOApp(
  name = "pdf-image-extractor",
  header = "Extract images from PDF files using pdfimages utility"
):

  val shiftOpt: Opts[Int] = Opts.option[Int](
    long = "shift", 
    help = "Shift numbering of output files"
  ).withDefault(0)
  
  val pdfFilesOpt: Opts[List[String]] = Opts.arguments[String](
    metavar = "pdf-files"
  ).map(_.toList)
  
  val mainOpts: Opts[(Int, List[Path])] = (shiftOpt, pdfFilesOpt).mapN { (shift, pdfFiles) => 
    (shift, pdfFiles.map(path => Path(path)))
  }
  
  def main: Opts[IO[ExitCode]] = mainOpts.map { case (shift, pdfFiles) =>
    processPdfFiles(pdfFiles, shift).as(ExitCode.Success)
  }
  
  def processPdfFiles(pdfFiles: List[Path], shift: Int): IO[Unit] =
    pdfFiles.parTraverseN(Runtime.getRuntime.availableProcessors()) { pdfFile =>
      processFile(pdfFile, shift).attempt.flatMap {
        case Right(_) => IO.println(s"Successfully processed: ${pdfFile.fileName}")
        case Left(e) => IO.println(s"Error processing ${pdfFile.fileName}: ${e.getMessage}")
      }
    }.void

  def processFile(pdfFile: Path, shift: Int): IO[Unit] =
    for
      exists <- Files[IO].exists(pdfFile)
      _ <- if !exists then IO.raiseError(new IllegalArgumentException(s"File does not exist: $pdfFile"))
          else IO.unit
          
      isPdf <- IO.pure(pdfFile.fileName.toString.toLowerCase.endsWith(".pdf"))
      _ <- if !isPdf then IO.raiseError(new IllegalArgumentException(s"Not a PDF file: $pdfFile"))
          else IO.unit
      
      baseName = getBaseName(pdfFile)
      
      parent = pdfFile.parent.getOrElse(Path("."))
      finalDir = parent / s"${baseName}_IMAGES"
      
      _ <- Files[IO].createDirectories(finalDir)
      
      result <- Files[IO].tempDirectory.use { tmpDir =>
        for
          outputPathPrefix <- (tmpDir / baseName).toString.pure[IO]
          imagePaths <- executePdfImagesCommand(pdfFile, outputPathPrefix)
          
          _ <- imagePaths.zipWithIndex.parTraverseN(Runtime.getRuntime.availableProcessors()) { 
            case (filePath, index) => moveExtractedFile(Path(filePath), index, shift, finalDir)
          }
        yield ()
      }
    yield result

  def getBaseName(pdfFile: Path): String =
    val fileName = pdfFile.fileName.toString
    if fileName.toLowerCase.endsWith(".pdf") then
      fileName.dropRight(4)
    else
      fileName
  
  def executePdfImagesCommand(pdfFile: Path, outputPathPrefix: String): IO[List[String]] =
    val command = ProcessBuilder("pdfimages", "-all", "-print-filenames", pdfFile.toString, outputPathPrefix)
    
    command.spawn[IO].use { process =>
      val outputLines = process.stdout
        .through(text.utf8.decode)
        .through(text.lines)
        .filter(_.nonEmpty)
        .map(line => line.split("\\s+").lastOption.getOrElse(""))
        .filter(_.nonEmpty)
        .compile
        .toList
      
      process.exitValue.flatMap { exitCode =>
        if exitCode == 0 then
          outputLines
        else
          process.stderr
            .through(text.utf8.decode)
            .compile
            .string
            .flatMap(err => IO.raiseError(
              new RuntimeException(s"pdfimages command failed with exit code: $exitCode\nError: $err")
            ))
      }
    }
  
  def moveExtractedFile(filePath: Path, index: Int, shift: Int, destinationDir: Path): IO[Unit] =
    val extension = getExtension(filePath.fileName.toString)
    val newFileName = f"${index + shift}%03d$extension"
    val destPath = destinationDir / newFileName
    
    Files[IO].exists(filePath).flatMap { exists =>
      if !exists then
        IO.raiseError(new RuntimeException(s"Extracted file not found: $filePath"))
      else
        Files[IO].exists(destPath).flatMap { destExists =>
          if destExists then
            IO.raiseError(new RuntimeException(s"File already exists: $destPath"))
          else
            Files[IO].move(filePath, destPath, CopyFlags(CopyFlag.AtomicMove))
        }
    }

  def getExtension(fileName: String): String =
    val lastDotIndex = fileName.lastIndexOf('.')
    if lastDotIndex >= 0 then
      fileName.substring(lastDotIndex)
    else
      ""
end PdfImageExtractor
