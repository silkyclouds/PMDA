import { useState } from 'react';
import { Loader2, Trash2 } from 'lucide-react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Header } from '@/components/Header';
import { ScanHistoryList } from '@/components/scan-history/ScanHistoryList';
import { ScanDetails } from '@/components/scan-history/ScanDetails';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog';
import { Button } from '@/components/ui/button';
import * as api from '@/lib/api';

export default function ScanHistory() {
  const queryClient = useQueryClient();
  const [selectedScan, setSelectedScan] = useState<number | null>(null);
  const [clearOpen, setClearOpen] = useState(false);
  const [clearing, setClearing] = useState(false);
  const { data: history = [], isLoading } = useQuery({
    queryKey: ['scan-history'],
    queryFn: api.getScanHistory,
  });

  const handleClearHistory = async () => {
    setClearing(true);
    try {
      await api.clearScanHistory();
      queryClient.invalidateQueries({ queryKey: ['scan-history'] });
      setSelectedScan(null);
      setClearOpen(false);
    } finally {
      setClearing(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  return (
    <>
      <Header />
      <div className="container mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">History</h1>
          <p className="text-muted-foreground mt-1">
            Scans, undupes, and restore moved albums
          </p>
        </div>
        <AlertDialog open={clearOpen} onOpenChange={setClearOpen}>
          <AlertDialogTrigger asChild>
            <Button variant="outline" size="sm" disabled={history.length === 0}>
              <Trash2 className="w-4 h-4 mr-2" />
              Clear history
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Clear scan history?</AlertDialogTitle>
              <AlertDialogDescription>
                This will permanently remove all scan and undupe entries from the history. Restore data for past moves will no longer be available. This cannot be undone.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel disabled={clearing}>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={(e) => { e.preventDefault(); handleClearHistory(); }}
                disabled={clearing}
                className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              >
                {clearing ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Clearing…
                  </>
                ) : (
                  'Clear history'
                )}
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </div>

      {/* List and Details */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <ScanHistoryList
          history={history}
          selectedScan={selectedScan}
          onSelectScan={setSelectedScan}
        />
        {selectedScan && (
          <ScanDetails
            scanId={selectedScan}
            onRestore={() => {
              queryClient.invalidateQueries({ queryKey: ['scan-history'] });
              setSelectedScan(null);
            }}
          />
        )}
      </div>
    </div>
    </>
  );
}
