import { dirname, join } from "path";

import { promisify } from "util";
import { readdir } from "fs/promises";
import { createReadStream, createWriteStream } from "fs";

import { pipeline, Transform } from "stream";
const pipelineAsync = promisify(pipeline);

// Lidar com os logs
import debug from "debug";
const log = debug("app:concat");

import csvToJson from "csvtojson";

// Obter o diretorio dos arquivos independente de onde estejam
const { pathname: currentFile } = new URL(import.meta.url);
const cwd = dirname(currentFile);
const filesDir = `${cwd}/dataset`;
const output = `${cwd}/final.csv`;

console.time("concat-data"); // Contabiliza o tempo de execução do processo

// Arquivos csv
const files = (await readdir(filesDir)).filter(
  (item) => !!!~item.indexOf(".zip")
);

log(`processing ${files}`);

// Com o unref, quando todos os outros processos async acabarem ele morre junto
setInterval(() => process.stdout.write, 1000).unref();

const combinedStreams = createReadStream(join(filesDir, files[0]));
const finalStream = createWriteStream(output);
const handleStream = new Transform({
  transform: (chunk, encoding, cb) => {
    const data = JSON.parse(chunk);

    const output = {
      id: data.Respondent,
      country: data.Country,
    };

    log(`id: ${output.id}`);
    return cb(null, JSON.stringify(output));

    // Quando não chamamos o cbo, ele entende que não tem mais linhas pra ele
    // ler e ele para
  },
});

await pipelineAsync(combinedStreams, csvToJson(), handleStream, finalStream);

log(`${files.length} files merged! on ${output}`);
console.timeEnd("concat-data");
