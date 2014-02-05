(defun find-erlang-el (erl)
  (concat
   (car (file-expand-wildcards
         (concat
          (concat
           (file-name-directory (directory-file-name
                                 (file-name-directory erl)))
           "lib")
          "/erlang/lib/tools-*")))
   "/emacs/"))

(setq erlang-el (find-erlang-el (executable-find "erl")))
(print erlang-el)

(setq load-path (cons erlang-el load-path))
(require 'erlang-start)

(setq-default tab-width 4 indent-tabs-mode nil)

(print argv)

(defun fmt (filename)
  (find-file filename)
  (erlang-indent-region (point-min) (point-max))
  (delete-trailing-whitespace)
  (save-buffer)
  (kill-buffer))

(defun fmtall (files)
  (when files
    (fmt (car files))
    (print (car files))
    (fmtall (cdr files))))

(fmtall argv)

