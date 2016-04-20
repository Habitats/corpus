C:\Program Files (x86)\IntelSWTools\compilers_and_libraries\windows\redist\intel64\mkl

export LIBND4J_HOME=`pwd` && bash ./buildnativeoperations.sh blas cpu
mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!org.nd4j:nd4j-cuda-7.5'


/usr/local/bin:/usr/bin:/bin:/opt/bin:/c/Program Files/ConEmu:/c/Program Files/ConEmu/ConEmu:/c/ProgramData/Oracle/Java/javapath:/c/WINDOWS/system32:/c/WINDOWS:/c/WINDOWS/System32/Wbem:/c/WINDOWS/System32/WindowsPowerShell/v1.0:/c/Program Files (x86)/AMD/ATI.ACE/Core-Static:/c/Program Files/OpenVPN/bin:/c/Program Files (x86)/ATI Technologies/ATI.ACE/Core-Static:/c/Program Files/apache-maven-3.3.3/bin:/c/Program Files/nodejs:/c/Program Files/Sublime Text 3:/c/Program Files (x86)/sbt/bin:/c/Program Files (x86)/scala/bin:/c/gradle-2.10/bin:/c/Program Files/Java/jdk1.8.0_60/bin:/c/spark/bin:/mingw64/bin:/usr/bin:/c/Program Files (x86)/IntelSWTools/compilers_and_libraries/windows/redist/intel64/compiler:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/intel64_win/mpirt:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/intel64_win/compiler:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/ia32_win/mpirt:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/ia32_win/compiler:/:/c/OpenBLAS:/c/Users/mail/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin:/c/Users/mail/AppData/Roaming/npm

export PATH=/usr/local/bin:/usr/bin:/bin:/opt/bin:/c/Program Files/ConEmu:/c/Program Files/ConEmu/ConEmu:/c/ProgramData/Oracle/Java/javapath:/c/WINDOWS/system32:/c/WINDOWS:/c/WINDOWS/System32/Wbem:/c/WINDOWS/System32/WindowsPowerShell/v1.0:/c/Program Files (x86)/AMD/ATI.ACE/Core-Static:/c/Program Files/OpenVPN/bin:/c/Program Files (x86)/ATI Technologies/ATI.ACE/Core-Static:/c/Program Files/apache-maven-3.3.3/bin:/c/Program Files/nodejs:/c/Program Files/Sublime Text 3:/c/Program Files (x86)/sbt/bin:/c/Program Files (x86)/scala/bin:/c/gradle-2.10/bin:/c/Program Files/Java/jdk1.8.0_60/bin:/c/spark/bin:/mingw64/bin:/usr/bin:/c/Program Files (x86)/IntelSWTools/compilers_and_libraries/windows/redist/intel64/compiler:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/intel64_win/mpirt:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/intel64_win/compiler:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/ia32_win/mpirt:/c/Program Files (x86)/Common Files/Intel/Shared Libraries/redist/ia32_win/compiler:/:/c/OpenBLAS:/c/Users/mail/AppData/Local/Google/Cloud SDK/google-cloud-sdk/bin:/c/Users/mail/AppData/Roaming/npm


# jemalloc 
pacman -S mingw64/mingw-w64-x86_64-jemalloc

# Build nd4j with
mvn clean install -DskipTests -Dmaven.javadoc.skip=true -pl '!:nd4j-cuda-7.5,!:nd4j-tests' -Djavacpp.compilerOptions='-DNATIVE_ALLOCATOR=je_malloc,-DNATIVE_DEALLOCATOR=je_free,-ljemalloc,-include,c:\msys64\mingw64\include\jemalloc\jemalloc.h'