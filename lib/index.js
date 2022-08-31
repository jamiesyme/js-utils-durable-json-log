import { open, readFile } from 'node:fs/promises'
import {
	ReadableStream,
	TransformStream,
	WritableStream,
} from 'node:stream/web'


function serializeEntry (entry)
{
	return JSON.stringify(entry)
}

function deserializeEntry (str)
{
	return JSON.parse(str)
}

function deserializeEntries (str)
{
	return str
		.split('\n')
		.map(s => s.trim())
		.filter(Boolean)
		.map(deserializeEntry)
}


export function createJsonLogEncoder (options)
{
	function transformer (entry, controller)
	{
		controller.enqueue(serializeEntry(entry) + '\n')
	}

	return new TransformStream(
		transformer,
		options?.writableStrategy,
		options?.readableStrategy,
	)
}

export function openJsonLog (path)
{
	// TODO: please, for the love of god, rewrite this without storing all of
	// the entries in memory...
	//
	// Here is an alternative solution that doesn't blow chunks:
	//
	// When creating a write stream, begin by reading the file and counting the
	// number of existing entries. After writing a new entry, increment the
	// count.
	//
	// When creating a read stream, read from file until the aforementioned
	// entry count has been reached. Then add a listener that the write stream
	// can notify whenever it successfully writes a new entry (assuming `follow`
	// is enabled).

	let entries = []
	let listeners = []
	let loader = null

	function addListenerOnce (fn)
	{
		function onceFn (...args)
		{
			try {
				fn(...args)
			} finally {
				removeListener()
			}
		}

		function removeListener ()
		{
			listeners = listeners.filter(l => l !== onceFn)
		}

		listeners.push(onceFn)

		return removeListener
	}

	async function autoLoad ()
	{
		if (loader) {
			return await loader
		}

		loader = (async () => {
			entries = deserializeEntries(await readFile(path, 'utf8'))
		})()
	}


	function createReadStream (options)
	{
		const { follow = false } = options ?? {}

		let index = 0
		let removeListener

		async function waitForNewEntry ()
		{
			return new Promise(resolve => {
				removeListener = addListenerOnce(resolve)
			})
		}

		return new ReadableStream({
			async start ()
			{
				await autoLoad()
			},

			async pull (controller)
			{
				if (index < entries.length) {
					controller.enqueue(entries[index++])
				} else if (follow) {
					await waitForNewEntry()
					controller.enqueue(entries[index++])
				} else {
					controller.close()
				}
			},

			cancel ()
			{
				removeListener?.()
			},
		})
	}

	function createWriteStream ()
	{
		let file

		return new WritableStream({
			async start ()
			{
				await autoLoad()
				file = await open(path, 'a')
			},

			async write (value)
			{
				// We stringify and re-parse the value before storing it in
				// memory to ensure that the behavior is as close as possible to
				// closing the log and re-opening it later. Otherwise, some edge
				// cases, such as date [de]serialization, may not get handled
				// correctly.

				let line = serializeEntry(value) + '\n'
				await file.appendFile(line, 'utf8')
				await file.datasync()

				value = deserializeEntry(str)
				entries.push(value)

				listeners.forEach(fn => fn(value))
			},

			async close ()
			{
				await file.close()
			},

			async abort ()
			{
				await file.close()
			},
		})
	}

	return {
		createReadStream,
		createWriteStream,
	}
}
